Tenzir now supports inline `if … else` expressions in the form `foo if pred`,
which returns `foo` if `pred` evaluates to `true`, or `null` otherwise, and `foo
if pred else bar`, which instead of falling back to `null` falls back to `bar`.
