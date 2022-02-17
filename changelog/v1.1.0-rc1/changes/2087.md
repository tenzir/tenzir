The `msgpack` encoding option is now deprecated. VAST issues a warning on
startup and automatically uses the `arrow` encoding instead. A future version of
VAST will remove this option entirely.

The experimental aging feature is now deprecated. If you require this feature,
please contact us about the [compaction
plugin](https://docs.tenzir.com/vast/features/compaction) which is its more
capable replacement.
