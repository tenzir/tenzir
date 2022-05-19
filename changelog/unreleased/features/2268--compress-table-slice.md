The `segment-store` store backend now compresses data before persisting it,
resulting in over 2x space savings for newly written data with the default VAST
configuration. This allowed us to increase the default partition size from
1'048'576 to 4'194'304 events, and the default number of events in a single
batch from 1024 to 65'536, yielding a significant performance increase at the
cost of a ~20% memory increase at peak load. We think this is a better default;
if you require less memory usage, reduce the value of `vast.max-partition-size`.
