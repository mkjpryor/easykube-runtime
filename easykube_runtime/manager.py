import asyncio
import typing as t

import easykube

from .controller import Controller, ObjectRequestMapper
from .reconciler import Reconciler
from .util import run_tasks
from .watch import Watch
from .worker_pool import WorkerPool


class Manager:
    """
    Manages the execution of multiple controllers with shared resources.
    """
    def __init__(
        self,
        *,
        # Optional namespace to limit controllers to
        namespace: t.Optional[str] = None,
        # The number of workers to use - only used if no worker pool is given
        worker_count: int = 10,
        # The worker pool to use
        worker_pool: t.Optional[WorkerPool] = None,
        # The maximum backoff to use before retrying when reconciliations fail
        requeue_max_backoff: int = 120
    ):
        self._namespace = namespace
        self._worker_pool = worker_pool or WorkerPool(worker_count)
        self._requeue_max_backoff = requeue_max_backoff
        self._controllers: t.List[Controller] = []
        self._watches: t.List[Watch] = []

    def register_controller(self, controller: Controller) -> t.Self:
        """
        Register the given controller with this manager.
        """
        self._controllers.append(controller)
        return self

    def create_controller(
        self,
        watch: Watch,
        reconciler: Reconciler,
        worker_pool: t.Optional[WorkerPool] = None,
        requeue_max_backoff: t.Optional[int] = None
    ) -> Controller:
        """
        Creates a new controller that is registered with this manager.
        """
        controller = Controller(
            watch,
            reconciler,
            worker_pool or self._worker_pool,
            requeue_max_backoff = requeue_max_backoff or self._requeue_max_backoff
        )
        self.register_controller(controller)
        return controller

    def register_watch(self, watch: Watch) -> t.Self:
        """
        Register the given watch with this manager.
        """
        self._watches.append(watch)
        return self

    def create_watch(
        self,
        api_version: str,
        kind: str,
        *,
        labels: t.Optional[t.Dict[str, easykube.LabelSelectorValue]] = None,
        namespace: t.Optional[str] = None
    ) -> Watch:
        """
        Creates a new watch that is registered with this manager.
        """
        watch = Watch(api_version, kind, labels = labels, namespace = namespace)
        self.register_watch(watch)
        return watch

    async def run(self, client: easykube.AsyncClient):
        """
        Run all the controllers registered with the manager using the given client.
        """
        # Run a task for each controller, one for each watch and one for the worker pool
        await run_tasks(
            [
                asyncio.create_task(controller.run(client))
                for controller in self._controllers
            ] + [
                asyncio.create_task(watch.run(client))
                for watch in self._watches
            ] + [
                asyncio.create_task(self._worker_pool.run()),
            ]
        )
