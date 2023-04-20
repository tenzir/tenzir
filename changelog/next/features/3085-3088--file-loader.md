The new `file` connector allows the user to ingest file output as input in a
pipeline. This includes regular files, UDS files as well as `stdout`. The
optional `--timeout <duration>` parameter specifies the timespan for
anticipating new data and the optional `--follow|-f` parameter appends new file
output, similar to `tail -f`.
