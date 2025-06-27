import logging
import types
import typing as t

import pydantic

import easykube
import easykube_runtime
import kube_custom_resource as kcr


def get_full_api_group(api_group: str, model: t.Type[kcr.CustomResource]) -> str:
    """
    Given an API group and a model, return the full API group.
    """
    return f"{model._meta.api_subgroup}.{api_group}" if model._meta.api_subgroup else api_group


def get_api_version(api_group: str, model: t.Type[kcr.CustomResource]) -> str:
    """
    Given an API group and a model, return the preferred API version.
    """
    return f"{get_full_api_group(api_group, model)}/{model._meta.version}"


async def register_crds(
    client: easykube.AsyncClient,
    api_group: str,
    models_module: types.ModuleType,
    *,
    categories: t.Optional[t.List[str]] = None,
    include_defaults: bool = False
) -> kcr.CustomResourceRegistry:
    """
    Discovers the models in the specified module, registers them with Kubernetes
    and returns the registry.
    """
    registry = kcr.CustomResourceRegistry(api_group, categories)
    registry.discover_models(models_module)
    for crd in registry:
        obj = crd.kubernetes_resource(include_defaults = include_defaults)
        await client.apply_object(obj)
    return registry


def create_watch(
    api_group: str,
    model: t.Type[kcr.CustomResource],
    *,
    labels: t.Optional[t.Dict[str, easykube.LabelSelectorValue]] = None,
    namespace: t.Optional[str] = None
) -> easykube_runtime.Watch:
    """
    Creates a watch for the specified model.
    """
    return easykube_runtime.Watch(
        get_api_version(api_group, model),
        model._meta.kind,
        labels = labels,
        namespace = namespace
    )


def map_owners(
    api_group: str,
    model: t.Type[kcr.CustomResource]
) -> easykube_runtime.ObjectRequestMapper:
    """
    Maps an object to requests to reconcile the owners of the specified model.
    """
    return easykube_runtime.mappers.owners(get_api_version(api_group, model), model._meta.kind)


TCustomResource = t.TypeVar("TCustomResource", bound = kcr.CustomResource)


class CustomResourceReconciler(easykube_runtime.Reconciler, t.Generic[TCustomResource]):
    """
    Base class for custom resource reconcilers.
    """
    def __init__(
        self,
        api_group: str,
        model: t.Type[TCustomResource],
        *,
        finalizer: t.Optional[str] = None
    ):
        self._api_group = api_group
        self._model = model
        # Use the full name of the resource as the finalizer
        self._finalizer = (
            finalizer or
            f"{model._meta.plural_name}.{get_full_api_group(api_group, model)}/finalizer"
        )

    async def reconcile_normal(
        self,
        client: easykube.AsyncClient,
        instance: TCustomResource,
        logger: logging.LoggerAdapter
    ) -> t.Tuple[TCustomResource, t.Optional[easykube_runtime.Result]]:
        """
        Reconcile the given instance and returns a tuple of the updated instance and the result.
        """
        raise NotImplementedError

    async def reconcile_delete(
        self,
        client: easykube.AsyncClient,
        instance: TCustomResource,
        logger: logging.LoggerAdapter
    ) -> t.Tuple[TCustomResource, t.Optional[easykube_runtime.Result]]:
        """
        Reconcile the deletion of the given instance and returns a tuple of the updated instance
        and the result.
        """
        raise NotImplementedError

    async def reconcile(
        self,
        client: easykube.AsyncClient,
        obj: t.Dict[str, t.Any],
        logger: logging.LoggerAdapter
    ) -> t.Optional[easykube_runtime.Result]:
        # First, parse the object using the model class
        # If it doesn't parse, discard the event
        try:
            instance = self._model.model_validate(obj)
        except pydantic.ValidationError:
            logger.exception("Object validation failed")
            return easykube_runtime.Result()
        # Reconcile the instance using the appropriate method
        # We apply or unapply the finalizer as required
        if not instance.metadata.deletion_timestamp:
            logger.info("Reconciling instance")
            instance = await self.ensure_finalizer(client, instance)
            _, result = await self.reconcile_normal(client, instance, logger)
            return result
        else:
            logger.info("Reconciling instance deletion")
            instance, result = await self.reconcile_delete(client, instance, logger)
            # If the delete was reconciled without a requeue, remove the finalizer
            if not result.requeue:
                _ = await self.remove_finalizer(client, instance)
            return result

    async def ensure_finalizer(
        self,
        client: easykube.AsyncClient,
        instance: TCustomResource
    ) -> TCustomResource:
        """
        Ensures that the specified finalizer is present on the given instance.
        The updated instance is returned.
        """
        if self._finalizer not in instance.metadata.finalizers:
            instance.metadata.finalizers.append(self._finalizer)
            return await self.save_instance(client, instance)
        else:
            return instance

    async def remove_finalizer(
        self,
        client: easykube.AsyncClient,
        instance: TCustomResource
    ) -> TCustomResource:
        """
        Ensures that the specified finalizer is not present on the given instance.
        The updated instance is returned.
        """
        try:
            idx = instance.metadata.finalizers.index(self._finalizer)
        except ValueError:
            return instance
        else:
            instance.metadata.finalizers.pop(idx)
            return await self.save_instance(client, instance)

    async def ekresource(
        self,
        client: easykube.AsyncClient,
        subresource: t.Optional[str] = None
    ) -> easykube.Resource:
        """
        Returns an easykube resource for the model that the reconciler is for.
        """
        api_version = get_api_version(self._api_group, self._model)
        resource = (
            f"{self._model._meta.plural_name}/{subresource}"
            if subresource
            else self._model._meta.plural_name
        )
        return await client.api(api_version).resource(resource)

    async def save_instance(
        self,
        client: easykube.AsyncClient,
        instance: TCustomResource,
        *,
        include_defaults: bool = False
    ) -> TCustomResource:
        """
        Saves the specified instance and returns the updated instance.
        """
        resource = await self.ekresource(client)
        data = await resource.replace(
            instance.metadata.name,
            instance.model_dump(exclude_defaults = not include_defaults),
            namespace = instance.metadata.namespace
        )
        return self._model.model_validate(data)

    async def save_instance_status(
        self,
        client: easykube.AsyncClient,
        instance: TCustomResource,
        *,
        include_defaults: bool = False
    ) -> TCustomResource:
        """
        Saves the status of the given instance and returns the updated instance.
        """
        resource = await self.ekresource(client, "status")
        data = await resource.replace(
            instance.metadata.name,
            {
                # Include the resource version for optimistic concurrency
                "metadata": { "resourceVersion": instance.metadata.resource_version },
                "status": instance.status.model_dump(exclude_defaults = not include_defaults),
            },
            namespace = instance.metadata.namespace
        )
        return self._model.model_validate(data)
