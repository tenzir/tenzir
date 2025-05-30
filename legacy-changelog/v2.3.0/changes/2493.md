The default value for `vast.active-partition-timeout` is now 5 minutes (down
from 1 hour), causing VAST to persist underful partitions earlier.

We split the `vast rebuild` command into two: `vast rebuild start` and `vast
rebuild stop`. Rebuild orchestration now runs server-side, and only a single
rebuild may run at a given time. We also made it more intuitive to use:
`--undersized` now implies `--all`, and a new `--detached` option allows for
running rebuilds in the background.
