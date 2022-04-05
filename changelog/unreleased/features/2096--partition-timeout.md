VAST now creates one active partition per layout, rather than having a single
active partition for all layouts.

The new option `vast.active-partition-timeout` controls the time after which an
active partition is flushed to disk, allowing for the `vast.max-partition-size`
option to be overruled. The active partition timeout defaults to 1 hour.
