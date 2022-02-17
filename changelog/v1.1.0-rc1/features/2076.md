The new builtin `rename` transform step allows for renaming event types as part
of a transformation. This is especially useful when you want to ensure that a
repeatedly triggered transformation does not affect already transformed events.

The new `aggregate` transform plugin allows for flexibly grouping and
aggregating events. We recommend using it alongside the [`compaction`
plugin](https://docs.tenzir.com/vast/features/compaction), e.g., for compaction
`suricata.flow` or `zeek.conn` events after a certain amount of time in the
database.
