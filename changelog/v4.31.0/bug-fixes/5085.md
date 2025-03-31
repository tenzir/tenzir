The binary operators `and` and `or` now skip evaluating their right-hand side
when not necessary. For example, `where this.has("foo") and foo == 42` now
avoids emitting a warning when `foo` does not exist.
