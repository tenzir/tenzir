The `export` command retrieves a subset of data according to a given query
expression. The export format must be explicitly specified:

```bash
vast export [options] <format> [options] [<query>]
```

This is easiest explained on an example:

```bash
vast export --max-events=100 --continuous json ':timestamp < 1 hour ago'
```

The above command outputs line-delimited JSON like this, showing one event per
line:

```json
{"ts": "2020-08-06T09:30:12.530972", "nodeid": "1E96ADC85ABCA4BF7EE5440CCD5EB324BEFB6B00#85879", "aid": 9, "actor_name": "pcap-reader", "key": "source.start", "value": "1596706212530"}
```

The above command signals the running server to export 100 events to the
`export` command, and to do so continuously (i.e., not matching data that was
previously imported). Only events that have a field of type `timestamp` will be
exported, and only if the timestamp in that field is older than 1 hour ago from
the current time at the node.

The default mode of operation for the `export` command is historical queries,
which exports data that was already archived and indexed by the node. The
`--unified` flag can be used to export both historical and continuous data.

The `--low-priority` option reduces the likelyhood that the query will be
selected for execution from the backlog. This will only have an effect when
there are more queries than query workers (configured with the `max-queries`
option), otherwise all queries are executed immediately and the backlog remains
empty.

For more information on the query expression, see the [query language
documentation](https://docs.tenzir.com/vast/query-language/overview).

The `<query>` expression may be provided on stdin, from a file or from
the command line. Specifying any two conflicting ways of reading the query
results in an error. Not providing a query causes VAST to export everything.

The table below gives an overview of all valid cases:

```
vast export <format> <query>
  //   takes the query from the command line
vast export -r - <format>
  //   reads the query from stdin.
echo "query" | vast export <format>
  //   reads the query from stdin
vast <query.txt export <format>
  //   reads the query from `query.txt`
vast export <format>
  //   export everything
 ```


Some export formats have format-specific options. For example, the `pcap` export
format has a `--flush-interval` option that determines after how many packets
the output is flushed to disk. A list of format-specific options can be
retrieved using the `vast export <format> help`, and individual documentation is
available using `vast export <format> documentation`.

