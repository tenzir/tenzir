The `cache` operator is a transformation that passes through events, creating an
in-memory cache of events on the first use. On subsequent uses, the operator
signals upstream operators no to start at all, and returns the cached events
immediately. The operator may also be used as a source for reading from a cache
only, or as a sink for writing to a cache only.
