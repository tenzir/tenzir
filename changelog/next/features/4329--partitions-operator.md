The `partitions [<expr>]` source operator replaces `show partitions` and now
supports an optional expression as a positional argument for showing only the
partitions that would be considered in `export | where <expr>`.
