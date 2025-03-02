We fixed a bug that caused pipelines with `from_fluent_bit` to not report their
startup successfully, causing errors when deploying pipelines starting with the
operator through the Tenzir Platform.
