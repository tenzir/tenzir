---
sidebar_position: 3
---

# Actor Model

Tenzir uses the [actor model][dist-prog-actor-model] to structure control flow
into individual components. The actor runtime maps the application logic onto
OS processes or remote actors in the network. The actor model simplifies the
design a distributed system because it allows for easier reasoning about
behavior, while providing a light-weight concurrency primitive that scales
remarkably well, both within a single machine as well as in a distributed
cluster.

## Execution Model

An actor defines a sequential unit of processing, while all actors conceptually
run in parallel. Because actors solely communicate via message passing, data
races do not occur by design. As long as the application exhibits enough
*overdecomposition* (i.e., distinct running actors), there exists enough "work"
that the actor runtime can schedule on OS-level threads. The figure below
illustrates the separation of application logic, actor runtime, and underlying
hardware. The programmer only thinks in actors (circles), and sending messages
between (arrows), whereas the runtime takes care of scheduling the actor
execution.

![Actor Execution Model](actor-execution-model.excalidraw.svg)

## C++ Actor Framework (CAF)

Tenzir is written in C++. We [evaluated multiple actor model library
implementations](http://matthias.vallentin.net/papers/thesis-phd.pdf) and
found that the [C++ Actor Framework (CAF)][caf] best suits our needs because of
the following unique features:

1. Efficient copy-on-write message passing
2. A configurable and exchangeable scheduler
3. Typed actor interfaces for compile-time message contract checking

### Efficient Message Passing

CAF's actor runtime transparently serializes messages when they cross process
boundaries; within a single process all actors send messages via efficient
pointer passing. The figure below illustrates this concept:

![Actor Message Passing](actor-message-passing.excalidraw.svg)

The main benefit of this capability is deployment flexibility: CAF decides when
to choose pointer passing and when serialization, based on whether an actor
runs remotely or within the same process. Without changing a single line of
code, we can create different wirings of components while retaining maximum
efficiency.

### Flexible Distribution

Letting the runtime transparently manage the messaging yields a highly flexible
distribution model. We can either bundle up all actors in a single process
(centralized) or scale out individual components into own processes
(distributed). Examples for these two modes include:

- **Centralized**: network appliance, embedded device at the edge, vertically
  scaled many-core machine
- **Distributed**: cloud functions, auto-scaling container environments (e.g.,
  Kubernetes), multi-node data-center clusters

You can think of two ends of the spectrum as follows:

![Actor Distribution](actor-distribution.excalidraw.svg)

### Actor Scheduling

By default, CAF uses a
[work-stealing](https://en.wikipedia.org/wiki/Work_stealing) scheduler to map
actors to threads. The idea is that there exist as many threads as available
CPU cores, but orders of magnitude more actors than threads. This results in a
steady flow of work, proportional to the amount of communication between the
actors. The figure below shows the scheduler in action.

![Actor Framework Workstealing](actor-workstealing.excalidraw.svg)

CAF maintains a thread pool, in which every thread maintains its own queue of
"work," i.e., dispatching control flow into an actor. If an actor sends a
message to another actor, CAF schedules the recipient. Based on how the worker
threads plow through their work, there may be scenario where a thread runs out
of work and ends up with an empty queue. In this case, *stealing* kicks in. The
idle worker (*thief*) picks an actor from the queue of another thread
(*victim*) so that all threads keep churning away. This works very efficiently,
assuming that stealing is a rare event.

[caf]: https://github.com/actor-framework/actor-framework
[dist-prog-actor-model]: http://dist-prog-book.com/chapter/3/message-passing.html#why-the-actor-model
