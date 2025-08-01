# easykube-runtime

`easykube-runtime` is a [Python](https://www.python.org/) package for developing
[Kubernetes](https://kubernetes.io/) operators following the
[operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/),
built on top of the [easykube](https://github.com/azimuth-cloud/easykube) Kubernetes
client.

It is heavily inspired by the
[Go controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) and the simple
model used there of events that trigger a reconciliation process.

## Concepts

There are a handful of fundamental components in `easykube-runtime`.

### Worker Pool

At the heart of an `easykube-runtime`-based operator is a worker pool. The worker pool controls
the maximum concurrency with which the operator can process reconciliation requests.

Controllers dispatch work to a worker pool to be executed when a worker becomes available.
A single worker pool is is shared across all the controllers registered with a manager.

Authors of `easykube-runtime`-based operators will not typically interact directly with the worker
pool, but it is important to know that it is there.

### Reconciler

Reconcilers are where the vast majority of the operator-specific logic lives.

A reconciler receives a Kubernetes object and is responsible for attempting to reconcile the state
of the world with desired state expressed in the object.

### Watch

A watch is responsible for watching a set of Kubernetes objects and emiting events to subscribers.
A single watch can emit events to multiple subscribers.

### Controller

A controller is responsible for managing the reconciliation of objects of a particular resource
using a reconciler.

A controller subscribes to one or more watches - one for the objects of the resource it is
reconciling and possibly others for related objects. It translates the watch events into
reconciliation requests (consisting of name and namespace) for objects of the resource it is
reconciling and queues them. It then dequeues those reconciliation requests in a controlled
fashion and schedules reconciliation tasks for those requests with the worker pool. When a
worker becomes available, the controller fetches the current state of the object being reconciled
and calls the reconciler with that object.

The controller is responsible for requeuing failed requests, which it does so with an exponential
backoff. It also ensures that only one reconciliation is in progress at a time for any specific
object.

### Manager

A manager is responsible for coordinating all the other components and providing the main
execution point for an operator. When watches and controllers are created via, or registered with,
a manager, the manager ensures that the worker pool is correctly shared and provides a single
`run` method that ensures all the watches and controllers are executed correctly.

## Writing reconcilers

## Setting up a manager

## `kube-custom-resource` integration
