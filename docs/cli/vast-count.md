The `count` command counts the number of events that a given query
expression yields. 

```bash
vast count [options] [<expr>]
```

For example:

```bash
vast count ':addr in 192.168.0.0/16'
```

This prints the number of events in the database that have an
address field in the subnet `192.168.0.0/16`.

An optional `--estimate` flag skips the candidate checks, i.e., asks only the
index and does not verify the hits against the database. This is a faster
operation and useful when an upper bound suffices.
