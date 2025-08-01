import asyncio
import collections
import typing as t

from .reconciler import Request


class Queue:
    """
    Queue of (request, attempt) tuples representing requests to reconcile objects.

    The queue is "smart" in a few ways:

      1. It has explicit operations for enqueuing a new request and requeuing a request that
         has previously been attempted.

      2. Requeuing of a request that has been previously attempted only happens after a delay.
         This happens asynchronously so that it does not block the worker from moving on to the
         next request.

      3. At most one request per object can be in the queue at any given time.

      4. Only one request per object is allowed to be "active" at any given time.
         The queue records when a request leaves the queue and does not allow any more requests
         for the same object to leave the queue until it has been notified that the
         previous request has been completed (either explicitly or by requeuing).
    """
    def __init__(self):
        # The requests in the queue, indexed by key
        self._requests: t.Dict[str, t.Tuple[Request, int]] = {}
        # Queue containing the keys in the order in which they should be processed
        # This allows us to efficiently replace the actual request for a key while maintaining
        # the order in which keys are processed to ensure no keys are starved
        self._queue: t.List[str] = []
        # A queue of futures
        # Each waiting "dequeuer" adds a future to the queue and waits on it
        # When a request becomes available, the first future in the queue is resolved, which
        # "wakes up" the corresponding dequeuer to read the request from the queue
        self._futures: t.Deque[asyncio.Future] = collections.deque()
        # A map of request key to request ID for active requests
        self._active: t.Dict[str, str] = {}
        # A map of request key to handles for requeue callbacks
        self._handles: t.Dict[str, asyncio.TimerHandle] = {}

    def _wakeup_next_dequeue(self):
        """
        Wake up the next eligible dequeuer by resolving the first future in the queue.
        """
        while self._futures:
            future = self._futures.popleft()
            if not future.done():
                future.set_result(None)
                break

    async def dequeue(self) -> t.Tuple[Request, int]:
        """
        Remove and return a request from the queue.

        If there are no requests that are eligible to leave the queue, wait until there is one.
        """
        while True:
            # Find the index of the first key in the queue for which there is no active task
            idx = next((i for i, key in enumerate(self._queue) if key not in self._active), -1)
            # If there is such a key, extract it from the queue and return it
            if idx >= 0:
                key = self._queue.pop(idx)
                request, attempt = self._requests.pop(key)
                # Register the request as having an active processing task
                self._active[request.key] = request.id
                return (request, attempt)
            else:
                # If there is no such key, wait to be woken up when the situation changes
                future = asyncio.get_running_loop().create_future()
                self._futures.append(future)
                await future

    def _cancel_requeue(self, key: str):
        # Cancel and discard any requeue handle for the same key
        handle = self._handles.pop(key, None)
        if handle:
            handle.cancel()

    def _do_enqueue(self, request: Request, attempt: int):
        # Cancel any pending requeues for the same key
        self._cancel_requeue(request.key)
        # Check if there is already a request with the same key in the queue
        if request.key in self._requests:
            # If the key is already in the queue, keep the request with the fewest retries
            # The key stays at the same place in the queue
            _, current_attempt = self._requests[request.key]
            if attempt <= current_attempt:
                self._requests[request.key] = (request, attempt)
        else:
            # If the key is not in the queue, add it to the back
            self._requests[request.key] = (request, attempt)
            self._queue.append(request.key)
        # Wake up the next waiting dequeuer
        self._wakeup_next_dequeue()

    def enqueue(self, request: Request):
        """
        Add a new request to the queue.
        """
        return self._do_enqueue(request, 0)

    def requeue(self, request: Request, attempt: int, delay: int):
        """
        Requeue a request after the specified delay.
        """
        # If there is already an existing requeue handle, cancel it
        self._cancel_requeue(request.key)
        # If there is already a request with the same key on the queue, there is nothing to do
        # If not, schedule a requeue after a delay
        #
        # NOTE(mkjpryor)
        # We use a callback rather than a task to schedule the requeue
        # This is because it allows us to cancel the requeue cleanly without trapping
        # CancelledError, allowing the controller as a whole to be cancelled reliably
        if request.key not in self._requests:
            # Schedule the requeue for the future and stash the handle
            self._handles[request.key] = asyncio.get_running_loop().call_later(
                delay,
                self._do_enqueue,
                request,
                attempt
            )
        # If a request is being requeued, assume the processing is complete
        self.processing_complete(request)

    def processing_complete(self, request: Request):
        """
        Indicates to the queue that processing for the given request is complete.
        """
        # Only clear the active record if the request ID matches
        if request.key in self._active and self._active[request.key] == request.id:
            self._active.pop(request.key)
            # Clearing the key may make another request eligible for processing
            self._wakeup_next_dequeue()
