The `cache` operator is a transformation that passes through events, creating an
in-memory cache of events on the first use. On subsequent uses, the operator
signals upstream operators no to start at all, and returns the cached events
immediately. The operator may also be used as a source for reading from a cache
only, or as a sink for writing to a cache only.

The `/pipeline/launch` operator features two new parameters `cache_id` and
`cache_capacity`. If a `cache_id` is specified, the pipeline's implicit sink
will use the `cache` operator under the hood. At least one of `serve_id` and
`cache_id` must be specified.
