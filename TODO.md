====
TODO
====

Ingestion
---------

  - GeoIP filter upon receiving events from an event source.

  - Check incoming events against the schema to ensure compatibility. When
    events arrive at the ingestor, create a CRC32 checksum of the event
    *types*. This requires available checksums of all events in the
    schema, which have to be created during the parsing process.

Schema
------

  - An `&ignore` attribute in the schema to skip indexing of either a
  	particular argument or an entire event

  - Schema evolution. For each each meta-event and type, save the last
    modification timestamp and then pre-filter events according to this
    timestamp. That is, before executing the query, restrict it to the valid
    interval. To allow for queries of each event/type revision, an extension of
    this would be to save a list of timestamps rather than just a single one.

Core
----

  - Create filesystem abstraction layer, vast::fs, that allows for using
    different filesystem at compile time, e.g., a DFS (HDFS, CloudStore) or a
    local filesystem (via `boost::filesystem`).

Query
-----

  - Live query architecture similar to YFilter: the set of all active live
    queries is represented by an NFA. Incoming events are sent through this NFA
    until all possible accepting states have been reached. Adding/removing a
    live query requires a change of the NFA. Performance optimization: compile
    NFA down to a DFA.

  - Provide a mechanism to understand what the granularity of information
    that is required to detect the important breaches. Not only do we need a
    history of queries, but this history needs to be augmented with attributes
    such as "yes, the query returned what I was interested in". In this fashion,
    it is possible to identify what information lays stale and what information
    is highly useful to store. In combination with Bro's access tracking of
    script variables, this could be a powerful cornerstone.

  - For extractor-based Schema-based queries, obtain a set of indices to
  	extract a specific record field. E.g., `c$id$resp_p` may translate into the
  	index set (0, 1, 3). Currently, schema-based extractors take linear time
  	*O(k)* where *k* is the location of the queried field in the flattened
  	argument list. The sketched approach would reduce it to *O(d)* where *d* is
  	the record depth or dimensionality of the index vector.
