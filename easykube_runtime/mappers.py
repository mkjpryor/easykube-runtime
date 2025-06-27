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


def owners(api_version: str, kind: str) -> t.Callable[[t.Dict[str, t.Any]], t.Iterable[Request]]:
    """
    Maps an object to a request to reconcile the owners of the specified kind.
    """
    def mapper(obj: t.Dict[str, t.Any]) -> t.Iterable[Request]:
        namespace = obj["metadata"].get("namespace")
        return [
            Request(ref["name"], namespace)
            for ref in obj["metadata"].get("ownerReferences", [])
            if ref["apiVersion"] == api_version and ref["kind"] == kind
        ]

    return mapper


def labels(
    name_label: str,
    include_namespace: bool = True,
    namespace_label: t.Optional[str] = None
) -> t.Callable[[t.Dict[str, t.Any]], t.Iterable[Request]]:
    """
    Maps an object to a request based on a label values and optional namespace label.

    The namespace is only included in the request if include_namespace = True (the default).
    If no namespace label is specified, the namespace of the object is used.
    """
    def mapper(obj: t.Dict[str, t.Any]) -> t.Iterable[Request]:
        # Extract the name and namespace from the object
        labels = obj["metadata"].get("labels", {})
        name = labels.get(name_label)
        namespace = labels.get(namespace_label) or obj["metadata"].get("namespace")
        # If the required information is present, return a request for the object
        # If either piece of information is missing when it is required, yield no requests
        if name and (not include_namespace or namespace):
            return [Request(name, namespace if include_namespace else None)]
        else:
            return []

    return mapper
