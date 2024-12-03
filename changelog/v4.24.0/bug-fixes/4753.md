The last metric emitted for each run of the `enrich` operator was incorrectly
named `tenzir.enrich.metrics` instead of `tenzir.metrics.enrich`, causing it not
to be available via `metrics enrich`.
