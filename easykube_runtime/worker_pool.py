import asyncio
import logging
import typing as t
import uuid

from .util import run_tasks


logger = logging.getLogger(__name__)


Task = t.Tuple[
    t.Callable[..., t.Awaitable[None]],
    t.Iterable[t.Any],
    t.Dict[str, t.Any]
]


class TaskCancelHandle:
    """
    Cancellation handle for a task.
    """
    def __init__(self, pool: 'WorkerPool', task_id: str):
        self._pool = pool
        self._task_id = task_id

    @property
    def task_id(self):
        """
        The ID of the task that this cancellation handle is for.
        """
        return self._task_id

    def cancel(self):
        """
        Cancel the task referred to by this handle.
        """
        self._pool.cancel(self)


class WorkerPool:
    """
    Represents a worker pool.
    """
    def __init__(self, worker_count):
        self._worker_count = worker_count
        # A map of task ID to task for the tasks in the queue
        # To remove a task from the queue, it is sufficient to remove it from this map
        # When the task ID reaches the top of the queue it will be ignored if the task is gone
        self._tasks: t.Dict[str, Task] = {}
        # A queue of task IDs in the order they should be processed
        self._queue: asyncio.Queue[str] = asyncio.Queue()

    def schedule(
        self,
        func: t.Callable[..., t.Awaitable[None]],
        *args: t.Any,
        **kwargs: t.Any
    ) -> TaskCancelHandle:
        """
        Schedule the specified task to run when a worker becomes available.

        Returns an object that can be used to cancel the task.
        """
        task_id = str(uuid.uuid4())
        self._tasks[task_id] = (func, args, kwargs)
        self._queue.put_nowait(task_id)
        return TaskCancelHandle(self, task_id)

    def cancel(self, handle: TaskCancelHandle):
        """
        Cancel the task referred to by the given cancellation handle.

        Note that this only allows tasks in the queue to be cancelled. Once a task
        has begun executing it cannot (currently) be cancelled.
        """
        self._tasks.pop(handle.task_id, None)

    async def _worker(self, id: int):
        """
        Run a single worker.
        """
        # Just continuously pull a task from the queue and process it
        while True:
            task_id = await self._queue.get()
            task = self._tasks.pop(task_id, None)
            if task:
                func, args, kwargs = task
                try:
                    await func(*args, **kwargs, worker_id = id)
                except:
                    logger.exception("exception occurred during task execution")
            self._queue.task_done()

    async def run(self):
        """
        Run the workers in the pool.
        """
        await run_tasks(
            [
                asyncio.create_task(self._worker(worker_id))
                for worker_id in range(self._worker_count)
            ]
        )
