The `distinct` function silently performed a different operation on lists,
returning the distinct non-null elements in the list rather than operating on
the list itself. This special-casing no longer exists, and instead the function
now operates on the lists itself. This feature will return in the future as
unnesting on the extractor level via `distinct(field[])`, but for now it has to
go to make the `distinct` aggregation function work consistently.
