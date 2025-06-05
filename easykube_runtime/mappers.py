import typing as t

from .reconciler import Request


def identity(obj: t.Dict[str, t.Any]) -> t.Iterable[Request]:
    """
    Maps an object to a request to reconcile itself.
    """
    return [
        Request(
            obj["metadata"]["name"],
            obj["metadata"].get("namespace")
        ),
    ]
