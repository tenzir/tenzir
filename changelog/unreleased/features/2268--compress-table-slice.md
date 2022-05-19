VAST now compresses data before persisting it or sending it between processes,
resulting in over 2x space savings for newly written data using the
`segment-store` store backend with the default VAST configuration, and resulting
in an up to 5x reduction of transferred data between `vast import` processes and
the VAST server. This allowed us to increase the default partition size from
1'048'576 to 4'194'304 events, and the default number of events in a single
batch from 1024 to 65'536, yielding a significant performance increase at the
cost of a ~20% memory increase at peak load. We think this is a better default;
if you require less memory usage, reduce the value of `vast.max-partition-size`.
