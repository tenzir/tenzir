# summarize

The `summarize` operator bundles input records according to a grouping
expression and applies an aggregation function over the each group.

The extent of a group depends on the pipeline input. For import and export
pipelines, a group comprises a single batch (configurable as
`vast.import.batch-size`). For compaction, a group comprises an entire partition
(configurable as `vast.max-partition-size`).

## Parameters

The `summarize` operator has grouping and aggregation options. The general
structure looks as follows:

```yaml
summarize:
  group-by:
    # inputs
  time-resolution:
    # bucketing for temporal grouping
  aggregate:
    # output 
```

### Grouping

The `group-by` option specifies a of
[extractors](/docs/understand-vast/query-language/expressions#extractors) that
should form a group. VAST internally calculates the combined hash for all
extractors for every row and puts the data into buckets for subsequent
aggregation.

### Time Resolution

The `time-resolution` option specifies an optional duration value that specifies
the tolerance when comparing time values in the `group-by` section. For example,
`01:48` is rounded down to `01:00` when a 1-hour `time-resolution` is used.

### Aggregate Functions

Aggregate functions compute a single value of one or more columns in a given
group. Fields that neither occur in an aggregation function nor in the
`group-by` list are dropped from the output.

The following aggregation functions are available:
- `sum`: Computes the sum of all grouped values.
- `min`: Computes the minimum of all grouped values.
- `max`: Computes the maxiumum of all grouped values.
- `any`: Computes the disjunction (OR) of all grouped values. Requires the
  values to be booleans.
- `all`: Computes the conjunction (AND) of all grouped values. Requires the
  values to be booleans.
- `distinct`: Creates a sorted list of all unique grouped values. If the values
  are lists, operates on the all values inside the lists rather than the lists
  themselves.
- `sample`: Takes the first of all grouped values.

## Example

```yaml
summarize:
  group-by:
    - timestamp
    - proto
    - event_type
  time-resolution: 1 hour
  aggregate:
    timestamp_min:
      min: timestamp
    timestamp_max:
      max: timestamp
    pkts_toserver: sum
    pkts_toclient: sum
    bytes_toserver: sum
    bytes_toclient: sum
    start: min
    end: max
    alerted: any
    ips:
      distinct:
        - src_ip
        - dest_ip
```
