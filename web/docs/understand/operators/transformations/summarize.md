# summarize

The `summarize` operator bundles input records according to a grouping
expression and applies an aggregation function over each group.

The extent of a group depends on the pipeline input. For import and export
pipelines, a group comprises a single batch (configurable as
`vast.import.batch-size`). For compaction, a group comprises an entire partition
(configurable as `vast.max-partition-size`).

## Synopsis

```
summarize [FIELD=]AGGREGATION(EXTRACTOR[, …])[, …] by EXTRACTOR[, …] [resolution DURATION]
```

### Aggregation Functions

Aggregation functions compute a single value of one or more columns in a given
group. Fields that neither occur in an aggregation function nor in the `by` list
are dropped from the output.

The following aggregation functions are available:
- `sum`: Computes the sum of all grouped values.
- `min`: Computes the minimum of all grouped values.
- `max`: Computes the maxiumum of all grouped values.
- `any`: Computes the disjunction (OR) of all grouped values. Requires the
  values to be booleans.
- `all`: Computes the conjunction (AND) of all grouped values. Requires the
  values to be booleans.
- `distinct`: Creates a sorted list of all unique grouped values that are not
  null.
- `sample`: Takes the first of all grouped values that is not null.
- `count`: Counts all grouped values that are not null.
- `count_distinct`: Counts all distinct grouped values that are not null.

### Grouping

The `group-by` option specifies a list of
[extractors](../../expressions.md#extractors) that should form a group. VAST
internally calculates the combined hash for all extractors for every row and
puts the data into buckets for subsequent aggregation.

### Time Resolution

The `resolution` option specifies an optional duration value that specifies the
tolerance when comparing time values in the `group-by` section. For example,
`01:48` is rounded down to `01:00` when a 1-hour `resolution` is used.

## Example

Show all distinct `id.origin_port` values grouped by `id.origin_ip` values.

```
summarize distinct(id.origin_port) by id.origin_ip
```

Show all distinct `id.origin_port` values grouped by `id.origin_ip` values in
a field with the custom name `total_ports`.

```
summarize total_ports=distinct(id.origin_port) by id.origin_ip
```

Show the result of `any(Initiated)` grouped by the `SourceIp, SourcePort,
DestinationPoint` and `UtcTime` values, with an optional time resolution of one
minute.

```
summarize any(Initiated) by SourceIp, SourcePort, DestinationPoint, UtcTime resolution 1 minute
```
