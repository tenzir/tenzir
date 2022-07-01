# Summarize Plugin for VAST

The `summarize` plugin for VAST adds a generic aggregation transformation step
that allows for flexible configuration. This is best used in conjunction with
the [`compaction` plugin][docs-compaction], and operates on the scope of the
transformation, which for import and export is the size of a single batch
(configurable as `vast.import.batch-size`), and for compaction is the size of a
partition (configurable as `vast.max-partition-size`).

[docs-compaction]: https://docs.tenzir.com/vast/features/compaction

## Configuration Options

The `summarize` transform step has multiple configuration options. Configuration
options that refer to fields support suffix matching, using the same syntax and
underlying mechanism that users already know from queries.

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
- `union`: Creates a list of all unique grouped values. If the values are lists,
  operates on the all values inside the lists rather than the values themselves.

## Usage Example

The below files configure VAST with the summarize and compaction plugins to
compact `suricata.flow` events after seven days by aggregating them into
`suricata.aggregated_flow` events. The grouping takes into account the timestamp
rounded to a full minute, the source address, the destination address and port,
and the protocol.

```yaml
# ${HOME}/.config/vast/vast.yaml
vast:
  plugins:
    - summarize
    - compaction

  transforms:
    suricata-flow-aggregate:
      - summarize:
          group-by:
            - suricata.flow.timestamp
            - suricata.flow.src_ip
            - suricata.flow.dest_ip
            - suricata.flow.dest_port
            - suricata.flow.proto
            - suricata.flow.event_type
          time-resolution: 1 minute
          sum:
            - suricata.flow.pcap_cnt
            - suricata.flow.flow.pkts_toserver
            - suricata.flow.flow.pkts_toclient
            - suricata.flow.flow.bytes_toserver
            - suricata.flow.flow.bytes_toclient
          min:
            - suricata.flow.flow.start
          max:
            - suricata.flow.flow.end
          any:
            - suricata.flow.flow.alerted
      - replace:
          field: event_type
          value: aggregated_flow
      - rename:
          schemas:
            - from: suricata.flow
              to: suricata.aggregated_flow
```

```yaml
# ${HOME}/.config/vast/plugins/compaction.yaml
time:
  rules:
    - name: compact-suricata-flow
      after: 7 days
      transform: suricata-flow-aggregate
      types:
        - suricata.flow
```
