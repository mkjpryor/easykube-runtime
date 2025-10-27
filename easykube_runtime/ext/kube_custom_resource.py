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


class CustomResourceReconciler(easykube_runtime.BaseReconciler, t.Generic[TCustomResource]):
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
        super().__init__(
            finalizer or
            f"{model._meta.plural_name}.{get_full_api_group(api_group, model)}/finalizer"
        )

    async def reconcile_instance_normal(
        self,
        client: easykube.AsyncClient,
        instance: TCustomResource,
        logger: logging.LoggerAdapter
    ) -> t.Tuple[TCustomResource, t.Optional[easykube_runtime.Result]]:
        """
        Reconcile the given instance and returns a tuple of the updated instance and the result.
        """
        raise NotImplementedError

    async def reconcile_instance_delete(
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

    async def reconcile_normal(
        self,
        client: easykube.AsyncClient,
        obj: t.Dict[str, t.Any],
        logger: logging.LoggerAdapter
    ) -> t.Tuple[t.Dict[str, t.Any], t.Optional[easykube_runtime.Result]]:
        # Validate the incoming object
        instance = self._model.model_validate(obj)
        instance, result = await self.reconcile_instance_normal(client, instance, logger)
        # Dump the model on the way back out
        return instance.model_dump(exclude_defaults = True), result

    async def reconcile_delete(
        self,
        client: easykube.AsyncClient,
        obj: t.Dict[str, t.Any],
        logger: logging.LoggerAdapter
    ) -> t.Tuple[t.Dict[str, t.Any], t.Optional[easykube_runtime.Result]]:
        # Validate the incoming object
        instance = self._model.model_validate(obj)
        instance, result = await self.reconcile_instance_delete(client, instance, logger)
        # Dump the model on the way back out
        return instance.model_dump(exclude_defaults = True), result

    async def reconcile(
        self,
        client: easykube.AsyncClient,
        obj: t.Dict[str, t.Any],
        logger: logging.LoggerAdapter
    ) -> t.Optional[easykube_runtime.Result]:
        # Run the logic from the parent, but catch any validation errors
        try:
            return await super().reconcile(client, obj, logger)
        except pydantic.ValidationError:
            logger.exception("Object validation failed")
            return easykube_runtime.Result()

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
