# Components

VAST uses the [actor model](actor-model) to structure the application logic into
individual components. Each component maps to an actor, which has a strongly
typed messaging interface that the compiler enforces. All actor components
execute concurrently, but control flow within a component is sequential. It's
possible that a component uses other helper components internally to achieve
more concurrency.

In other words, a component is a microservice within the application. Most
components are multi-instance in that the runtime can spawn them multiple times
for horizontal scaling. Only a few components are singletons where at most one
instance can exist, e.g., because they guard access to an underlying resource.

The diagram below illustrates VAST's key components in the dataflow between
them:

![Components](/img/components.light.png#gh-light-mode-only)
![Components](/img/components.dark.png#gh-dark-mode-only)

By convention, we use ALL-CAPS naming for actor components and represent them as
circles. Red circles are singletons and blue circles multi-instance actors. We
also did not show process boundaries in this diagram, as the actor model allows
us to [draw them flexibly](actor-model#flexible-distribution), based on the
requirements of the deployment environment.

## Singleton Components

Singleton components have a special restriction in that VAST can spawn at most
one instance of them. This restriction exists because such a component mutates
an underlying resource and therefore needs to enforce sequential access. (An
actor does this by definition, because control flow within actor is sequential.)

### CATALOG

The catalog is the central component that sits in both read and write path. It
has the two key functions:

1. **Partition Management**: the catalog is the owner of *partitions*, each of
   which consists of a concatenation of record batches encoded in a format
   suitable for persistence, plus optional sparse and dense indexes. Other
   components can add and remove partitions. The catalog is the only component
   that allows mutation as part of its interface.
2. **Query Entry Point**: user queries arrive at the catalog, which returns a
   set of candidate partitions for each query by looking up partition metadata
   and, if available, performing sketch lookups. The result of query consists of
   URI that points to the partition and a small amount of partition metadata.
   The catalog also forwards a query to all partition builders for optimal
   result freshness.

### SCHEDULER

The scheduler is the central component in the query engine that drives query
execution. Scheduling concerns the loading and evicting in-memory partitions
that can answer queries concurrently, with the goal to achieve minimum partition
thrashing.

Why does thrashing occur? Typical workloads in security analytics (especially
when executing security content) exhibit a high rate of point queries. This
results in a large overlap of relevant partitions among a given set of queries.
In the most naive case of serial query execution, VAST would process all
partitions sequentially, i.e., loading one from disk, run the query, evict it
again. If we have a total of P partitions and Q queries waiting in line to
be executed, we would perform P ⨉ Q partition load and evict operations. In
practice, each query only needs to access a subset of partitions. The catalog is
the component that determines the candidate set for a given query. For an
increasing number of queries, the overlap of partitions turns out to be large.
This is where scheduler benefit kicks in: by sorting the to-be-processed
partitions by the number of queries outstanding queries that access them, we can
create optimal I/O access patterns.

## Multi-instance Components

Multi-instance components exist at various place in the path of the data. They
often operate stateless and implement pure (side-effect-free) functions. In case
they own state, there is no dependency to other state of the same instance. For
example, a component may operate on a single file, but the whole system operates
on many distributed files, each of which represented by a single instance.

### LOADER

:::note Not Yet Implemented
This component is not yet implemented. Until then, the [SOURCE](#SOURCE)
performs both I/O and subsequent input parsing.
:::

### SOURCE

The source transforms a stream of framed bytes into Arrow record batches, and
then relays them to a partition builder. A source actor wraps a pluggable
*reader* for a given input format, e.g., JSON, CSV, or PCAP.

### PARTITION BUILDER

The partition builder takes as input a sequence of Arrow record batches and
turns them into partitions. There exists one builder per schema, so the stream
of record batches gets demultiplexed over a set of builders. Each partition
builder keeps writing record batches into its store until either a timeout fires
or the store reaches a configured size. The builder then hands the ownership of
the resulting partition the catalog and starts over with a new partition.

In addition to translating the in-memory record batch representation into a
persistent format, the partition builder also generates sparse and dense
indexes to accelerate queries.

:::note Not Yet Implemented
Partition building as described above is not happening at this location in the
dataflow pipeline, but deeper inside the query engine. Moreover, building of
dense bitmap indexes also takes place elsewhere and is not yet configurable. We
are in the process of refactoring this logic to match the described
architecture.
:::

### QUERY

Every query a user submits has a corresponding query actor that tracks its
execution. The interface of the query actor allows for extracting results in a
pull-based fashion, e.g., users can ask "give me 100 more results".

### SINK

The sink transforms a stream Arrow record batches into a sequence of bytes using
a pluggable *writer* for a given output format, e.g., JSON, CSV, or PCAP.

### DUMPER

:::note Not Yet Implemented
This component is not yet implemented. Until then, the [SINK](#SINK)
performs both output formatting and subsequent I/O.
:::
