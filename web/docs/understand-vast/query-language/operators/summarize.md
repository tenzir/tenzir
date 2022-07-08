# summarize

The `summarize` operator for VAST adds a generic transformation step that allows
for flexible grouping and aggregation.

The aggregation operates on the scope of the transformation, which for import
and export is the size of a single batch (configurable as
`vast.import.batch-size`), and for compaction is the size of a partition
(configurable as `vast.max-partition-size`).

## Parameters

The `summarize` transform step has multiple configuration options. In the places
where the configuration refers to a field name, it is also possible to specify a
suffix of a nested field instead spelling out the full name.

### Grouping

 The `group-by` option specifies a list of columns to group by. VAST internally
 calculates the combined hash for all the configured columns for every row, and
 sorts the data into buckets for aggregation.

 ### Time Resolution

 The `time-resolution` option specifies an optional duration value that specifies
 the tolerance when comparing time values in the `group-by` section. For example,
 `01:48` is rounded down to `01:00` when a 1-hour `time-resolution` is used.

 ### Aggregate Functions

 A list of columns to perform the respective aggregation function on the grouped
 buckets. Fields that have no such function specified and are not part of the
 `group-by` columns are dropped from the output.

 Here's how the individual aggregation functions operate:
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
