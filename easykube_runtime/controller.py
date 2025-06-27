import asyncio
import logging
import random
import typing as t

import easykube

from . import mappers
from .queue import Queue
from .reconciler import Reconciler, Request, Result
from .watch import Watch, WatchCallback, WatchEvent
from .worker_pool import WorkerPool


#: Type for a function that takes a Kubernetes object and returns an iterable of reconciliation
#: requests for objects of the controller type
ObjectRequestMapper = t.Callable[[t.Dict[str, t.Any]], t.Iterable[Request]]


class Controller:
    """
    Class for a controller that watches a resource and its related resources and calls a
    reconciliation function whenever an object needs to be reconciled.
    """
    def __init__(
        self,
        # The watch for the objects that the controller is reconciling
        watch: Watch,
        # The reconciler to use to reconcile objects
        reconciler: Reconciler,
        # The worker pool to use for reconciliation tasks
        worker_pool: WorkerPool,
        *,
        # The maximum backoff to use before retrying when reconciliations fail
        requeue_max_backoff: int = 120
    ):
        self._api_version = watch.api_version
        self._kind = watch.kind
        self._reconciler = reconciler
        self._worker_pool = worker_pool
        self._requeue_max_backoff = requeue_max_backoff
        self._queue = Queue()
        self.subscribe_watch(watch, mappers.identity)

    @property
    def api_version(self) -> str:
        """
        The API version of objects that the controller is reconciling.
        """
        return self._api_version

    @property
    def kind(self) -> str:
        """
        The kind of objects that the controller is reconciling.
        """
        return self._kind

    def _watch_callback(self, mapper: ObjectRequestMapper) -> WatchCallback:
        """
        Returns a watch callback for the given mapper function.
        """
        # Just run the mapper on the object and enqueue the resulting requests
        def callback(evt: WatchEvent):
            for request in mapper(evt["object"]):
                self._queue.enqueue(request)
        return callback

    def subscribe_watch(self, watch: Watch, mapper: ObjectRequestMapper) -> t.Self:
        """
        Subscribe to the specified watch using the specified mapper.
        """
        watch.subscribe(self._watch_callback(mapper))

    async def _fetch_obj(
        self,
        client: easykube.AsyncClient,
        request: Request
    ) -> t.Optional[t.Dict[str, t.Any]]:
        """
        Try to fetch the object specified in the request.

        If it does not exist, None is returned.
        """
        resource = await client.api(self.api_version).resource(self.kind)
        try:
            return await resource.fetch(request.name, namespace = request.namespace)
        except easykube.ApiError as exc:
            if exc.status_code == 404:
                return None
            else:
                raise

    async def _handle_request(
        self,
        client: easykube.AsyncClient,
        request: Request,
        attempt: int,
        *,
        worker_id: int
    ):
        """
        Start a worker that processes reconcile requests.
        """
        # Get a logger that populates parameters for the request
        logger = logging.LoggerAdapter(
            logging.getLogger(__name__),
            {
                "api_version": self.api_version,
                "kind": self.kind,
                "instance": request.key,
                "request_id": request.id,
                "worker_id": worker_id,
            }
        )
        logger.info("Handling reconcile request (attempt %d)", attempt + 1)
        # Try to reconcile the request
        try:
            # First, try to load the object
            obj = await self._fetch_obj(client, request)
            if obj:
                # Then try to reconcile the object
                result = await self._reconciler.reconcile(client, obj, logger)
            else:
                # Log the missing object and discard the event
                logger.warning("Object no longer exists")
                result = Result()
        except asyncio.CancelledError:
            # Propagate cancellations with no further action
            raise
        except Exception:
            logger.exception("Unexpected error while handling reconcile request")
            result = Result(True)
        else:
            # If the result is None, use the default result
            result = result or Result()
        # Work out whether we need to requeue or whether we are done
        if result.requeue:
            if result.requeue_after:
                delay = result.requeue_after
            else:
                delay = min(2**attempt, self._requeue_max_backoff)
            # Add some jitter to the requeue
            delay = delay + random.uniform(0, 1)
            logger.info("Requeuing request after %.3fs", delay)
            self._queue.requeue(request, attempt + 1, delay)
        else:
            logger.info("Successfully processed reconcile request")
            # Mark the processing for the request as complete
            self._queue.processing_complete(request)

    async def run(self, client: easykube.AsyncClient):
        """
        Run the controller using the given client.
        """
        # We just need to pull requests from the queue and dispatch them to the worker pool
        # Because of how the queue works, this means there will be at most one task per
        # object scheduled with the worker pool
        while True:
            # Spin until there is a request in the queue that is eligible to be dequeued
            request, attempt = await self._queue.dequeue()
            # Schedule the work to process the request
            self._worker_pool.schedule(self._handle_request, client, request, attempt)
