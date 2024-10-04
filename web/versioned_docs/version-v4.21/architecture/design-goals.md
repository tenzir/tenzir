---
sidebar_position: 1
---

# Design Goals

In the following we articulate the design goals that we believe are vital to
consider when building a security analytics platform. We separate the discussion
into data and architecture aspects.

:::note HotSec '08 Workshop
Many design goals are derivatives from the workshop paper in 2008 about
[Principles for Developing Comprehensive Network Visibility][hotsec08] by Mark
Allman, Christian Kreibich, Vern Paxson, Robin Sommer, and Nicholas Weaver.
:::

[hotsec08]: https://www.icir.org/mallman/papers/awareness-hotsec08.pdf

## Data

Data is in the center of every analytical system. We formulate the following
data-related design goals.

### Open Data Access

In a world of black boxes exposed through a narrow API, many organizations have
limited access to their very own data. We want to build a system that gives
the user full control over their data, and freedom of choice for processing
tools.

Importantly, data should not be encoded in a proprietary format. We do not want
vendor lock-in. For a security analytics platform, using open protocols and
standards for data is fundamental to build trust so that users can build
applications on top.

### Rich and Strong Typing

To accommodate the idioms of the security domain, an analytics engine must be
able to work with objects in the domain, without losing semantic information and
degenerating the representation to a generic data model. Security analysts are
not data engineers, and the goal should be avoiding context switches to
lower-level implementation details when possible.

For example, the [Zeek](https://zeek.org) security monitor provides first-class
support for domain-specific entities (e.g., native representation of IPv4 and
IPv6 addresses) and type-specific operations (e.g., the ability to perform top-k
prefix search to answer subnet membership queries). In addition, it must be
possible to extend the pre-defined types with user-defined types to allow for
customization or enhancement of domain semantics.

Keeping strong typing at the core of the system (as opposed to just at the data
exchange boundary) is important to allow type-specific optimizations and push
processing logic deeper into the system.

### Interoperability

Having an open and flexible data model is not enough for building a platform.
The system must also come with extensible mechanisms to integrate with *other*
systems. In fact, a platform should come with first-class support for
simplifying integration with existing solutions.

For example, this can mean offering multiple access mechanisms to the same data
that other tools already use, such as Kafka or REST APIs.

## System Architecture

For building a system that can effectively support security domain experts, we
formulate the following architecture-related design goals.

### Adaptive Storage

Security analytics operates on activity data, typically representing action of
entities. There is no need to modify a witnessed action, as it would change the
semantics of what happened. Therefore we must treat the corresponding telemetry
as immutable and store it an append-only manner.

But when storage is finite, old data must be deleted eventually. To maximize
retention spans and enable longitudinal analytics, the engine should support
more than just deletion of old data, e.g., implement incremental aging by
rolling up old data into more space-efficient representations. Operators should
be able to express retention policies declaratively, both for compliance use
cases and to filter out less useful data.

### Natively Scalable Runtime

We envision a distributed system that can perform the same processing
centralized in the cloud and deep at the edge. CPU, memory, and storage have
drastically different forms along this spectrum. Our goal is to deploy the same
engine across the entire spectrum, but with different pluggable components and
varying configurations that can adapt to the local environment. That is, the
system should scale vertically as well as horizontally.

### Separate Read and Write Path

Security analytics involves processing of structured event data that represents
activity of endpoints, the network, and cloud resources. This generates a
never-ending torrent of telemetry at high event rates. Consequently, an
analytics engine must be able to operate under a saturated write path (ingest).

The read path (queries) should not be affected by the write path and scale
independently, at least conceptually. In certain deployment environments this is
not avoidable, e.g., when the I/O path to persistent storage is shared, or
cannot handle well simultaneous read/write operations.
