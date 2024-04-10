Pipelines that are in `failed` state can now be explicitly changed to the
`stopped` state.

The new pipeline parameter `retry_delay` is a duration that measures a delay
for restarting a failed pipeline, beginning from the last pipeline start. If
the lifetime of a running pipeline extends this duration, restart upon failure
will be instantaneous.
