The `count` command counts a subset of data according to a given query
expression.  This is easiest explained on an example:

```bash
vast count ':addr in 192.168.0.0/16'
```

The above command prints the amount of events in the database which have an
address field in the subnet `192.168.0.0/16`.

An optional `--estimate` flag skips the candidate checks, i.e., asks only the
index and does not verify the hits against the database. This is a much quicker
operation and very useful in scenarios where an upper bound is required only.
