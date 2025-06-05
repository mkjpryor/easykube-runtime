import logging
import typing as t
import uuid

import easykube


logger = logging.getLogger(__name__)


class WatchEvent(t.TypedDict, total = False):
    """
    Type for events emitted by Kubernetes watches.
    """
    #: The type of the event
    type: t.Required[t.Literal["ADDED", "MODIFIED", "DELETED"]]
    #: The object that the event is for
    object: t.Required[t.Dict[str, t.Any]]


#: Type for a watch callback
WatchCallback = t.Callable[[WatchEvent], None]


#: Type for a watch cancellation handle
WatchCancelHandle = t.Callable[[], None]


class Watch:
    """
    Watches a Kubernetes resource and produces reconcile requests.
    """
    def __init__(
        self,
        api_version: str,
        kind: str,
        *,
        labels: t.Optional[t.Dict[str, easykube.LabelSelectorValue]] = None,
        namespace: t.Optional[str] = None
    ):
        self._api_version = api_version
        self._kind = kind
        self._labels = labels
        self._namespace = namespace
        # Callbacks indexed by subscription ID
        self._callbacks: t.Dict[str, WatchCallback] = {}

    @property
    def api_version(self) -> str:
        """
        The API version for the watch.
        """
        return self._api_version

    @property
    def kind(self) -> str:
        """
        The kind for the watch.
        """
        return self._kind

    def subscribe(self, callback: WatchCallback) -> WatchCancelHandle:
        """
        Subscribe a callback to the events for this watch.
        """
        subscription_id = str(uuid.uuid4())
        self._callbacks[subscription_id] = callback

        def cancel_subscription():
            self._callbacks.pop(subscription_id, None)

        return cancel_subscription

    def _emit(self, event: WatchEvent):
        """
        Emit the given event to all the subscribers.
        """
        for callback in self._callbacks.values():
            callback(event)

    async def run(self, client: easykube.AsyncClient):
        """
        Run the watch, pushing requests onto the specified queue.
        """
        resource = await client.api(self._api_version).resource(self._kind)
        watch_kwargs = {}
        if self._labels:
            watch_kwargs["labels"] = self._labels
        if self._namespace:
            watch_kwargs["namespace"] = self._namespace
        else:
            watch_kwargs["all_namespaces"] = True
        logger.info(
            "Starting watch",
            extra = {
                "api_version": self._api_version,
                "kind": self._kind,
            }
        )
        initial, events = await resource.watch_list(**watch_kwargs)
        # Emit synthetic MODIFIED events for each of the initial objects
        for obj in initial:
            self._emit({"type": "MODIFIED", "object": obj})
        async for event in events:
            self._emit(t.cast(WatchEvent, event))
