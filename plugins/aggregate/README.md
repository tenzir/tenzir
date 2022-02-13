# Aggregate Plugin for VAST

The `aggregate` plugin for VAST adds a generic aggregation transformation step
that allows for flexible configuration. This is best used in conjunction with
the [`compaction` plugin][docs-compaction].

[docs-compaction]: https://docs.tenzir.com/vast/features/compaction

## Configuration Options

The `aggregate` transform step has multiple configuration options. Configuration
options that refer to fields support suffix matching, using the same syntax and
underlying mechanism that users already know from queries.

### `group-by`

A list of columns to group by. VAST internally calculates the cross product for
all the configured columns for every row, and sorts the data into buckets for
aggregation.

### `round-temporal-multiple`

An optional duration value that specifies the tolerancy when comparing time
values in the `group-by` section.

### `sum` / `min` / `max` / `mean` / `any` / `all`

A list of columns to perform the respective computation on within the grouped
buckets. Fields that have no such compute operation and are not part of the
`group-by` columns are dropped from the output.

### `disable-eager-pass` / `disable-lazy-pass`

By default, the `aggregate` transform step computes the aggregation in two
passes:
- An eager pass when a batch of data arrives at the transformation, and
- a lazy pass when VAST requests the result of the transformation.

Either of the passes may be disabled depending on the use case. We highly
recommend leaving both passes enabled when using the `aggregate` transform with
the `compaction` plugin.

Note that if both passes are enabled, `mean` calculates the mean of the eager
result during the lazy pass, which may introduce rounding errors.

## Usage Example

The below files configure VAST with the aggregate and compaction plugins to
compact `suricata.flow` events after seven days by aggregating them into
`suricata.aggregated_flow` events. The grouping takes into account the timestamp
rounded to a full minute, the source address, the destination address and port,
and the protocol.

```yaml
# ${HOME}/.config/vast/vast.yaml
vast:
  plugins:
    - aggregate
    - compaction

  transforms:
    suricata-flow-aggregate:
      - aggregate:
          group-by:
            - suricata.flow.timestamp
            - suricata.flow.src_ip
            - suricata.flow.dest_ip
            - suricata.flow.dest_port
            - suricata.flow.proto
            - suricata.flow.event_type
          round-temporal-multiple: 1 minute
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
            - zeek.conn._write_ts
          any:
            - suricata.flow.flow.alerted
      - replace:
          field: event_type
          value: aggregated_flow
      - rename:
          layout-names:
            - from: suricata.flow
              to: suricata.aggregated_flow
```

```yaml
# ${HOME}/.config/vast/plugins/compaction.yaml
time:
  rules:
    - name: compact-suricata-flow
      after: 7 days
      transform: aggregate-suricata-flow
      types:
        - suricata.flow
```
