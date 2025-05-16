The `every` and `cron` operators now correctly function with operators that
access state at the node that lives outside of the pipeline, e.g.,
`context::update`. They are also no longer restricted to operator's that prefer
the same location.
