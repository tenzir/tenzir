The binary operators `and` and `or` no longer evaluate their right-hand side
when not necessary. For example, `where this.has("foo") and foo == 42` no longer
emits a warning when `foo` does not exist.
