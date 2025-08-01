import dataclasses
import logging
import typing as t
import uuid

import easykube


@dataclasses.dataclass(frozen = True)
class Request:
    """
    Represents a request to reconcile an object.
    """
    #: The name of the object to reconcile
    name: str
    #: The namespace of the object to reconcile, or none for cluster-scoped objects
    namespace: t.Optional[str] = None
    #: The ID of the request
    id: str = dataclasses.field(default_factory = lambda: str(uuid.uuid4()))

    @property
    def key(self):
        """
        The key for the request.
        """
        return f"{self.namespace}/{self.name}" if self.namespace else self.name


@dataclasses.dataclass(frozen = True)
class Result:
    """
    Represents the result of a reconciliation.
    """
    #: Indicates whether the request should be requeued
    requeue: bool = False
    #: Indicates the time in seconds after which the request should be requeued
    #: If not given, a clamped exponential backoff is used
    requeue_after: t.Optional[int] = None


class Reconciler:
    """
    Interface for reconcilers.
    """
    async def reconcile(
        self,
        client: easykube.AsyncClient,
        obj: t.Dict[str, t.Any],
        logger: logging.LoggerAdapter
    ) -> t.Optional[Result]:
        """
        Reconcile the current state of the given object.
        """
        raise NotImplementedError


class BaseReconciler(Reconciler):
    """
    Base class for reconcilers.
    """
    def __init__(self, finalizer: str):
        self._finalizer = finalizer

    async def reconcile_normal(
        self,
        client: easykube.AsyncClient,
        obj: t.Dict[str, t.Any],
        logger: logging.LoggerAdapter
    ) -> t.Tuple[t.Dict[str, t.Any], t.Optional[Result]]:
        """
        Reconcile the given object and return a tuple of the updated object and a result.
        """
        raise NotImplementedError

    async def reconcile_delete(
        self,
        client: easykube.AsyncClient,
        obj: t.Dict[str, t.Any],
        logger: logging.LoggerAdapter
    ) -> t.Tuple[t.Dict[str, t.Any], t.Optional[Result]]:
        """
        Reconcile the deletion of the given object and return a tuple of the updated object
        and a result.
        """
        raise NotImplementedError

    async def reconcile(
        self,
        client: easykube.AsyncClient,
        obj: t.Dict[str, t.Any],
        logger: logging.LoggerAdapter
    ) -> t.Optional[Result]:
        """
        Reconcile the current state of the given object.
        """
        deletion_timestamp = obj["metadata"].get("deletionTimestamp")
        if not deletion_timestamp:
            logger.info("Reconciling object")
            obj = await self.ensure_finalizer(client, obj)
            _, result = await self.reconcile_normal(client, obj, logger)
            return result
        else:
            logger.info("Reconciling object deletion")
            obj, result = await self.reconcile_delete(client, obj, logger)
            # If the delete was reconciled without a requeue, remove the finalizer
            if not result or not result.requeue:
                _ = await self.remove_finalizer(client, obj)
            return result

    async def ensure_finalizer(
        self,
        client: easykube.AsyncClient,
        obj: t.Dict[str, t.Any]
    ) -> t.Dict[str, t.Any]:
        """
        Ensure our finalizer is present on the given object.
        """
        finalizers = obj["metadata"].get("finalizers", [])
        if self._finalizer not in finalizers:
            return await client.replace_object(
                {
                    **obj,
                    "metadata": {
                        **obj["metadata"],
                        "finalizers": [
                            *finalizers,
                            self._finalizer
                        ],
                    },
                }
            )
        else:
            return obj

    async def remove_finalizer(
        self,
        client: easykube.AsyncClient,
        obj: t.Dict[str, t.Any]
    ) -> t.Dict[str, t.Any]:
        """
        Remove our finalizer from the given object.
        """
        finalizers = obj["metadata"].get("finalizers", [])
        if self._finalizer in finalizers:
            return await client.replace_object(
                {
                    **obj,
                    "metadata": {
                        **obj["metadata"],
                        "finalizers": [
                            f
                            for f in finalizers
                            if f != self._finalizer
                        ],
                    },
                }
            )
        else:
            return obj
