We fixed an issue where the `export`, `metrics`, or `diagnostics` operators
crashed the node when started while the node was shutting down or after an
unexpected filesystem error occurred. This happened frequently while using the
Tenzir Platform, which subscribes to metrics and diagnostics automatically.
