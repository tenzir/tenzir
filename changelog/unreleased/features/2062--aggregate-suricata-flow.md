# Aggregate Suricata Flow Plugin for VAST

This plugin aggregates Suricata flow data into a new aggregated layout. The
aggregation groups the input based on the `timestamp`, `src_ip`, `dest_ip`, `dest_port`,
`proto`, and the configured bucket size. The `timestamp` and the `bucket-size`
together determine the time group. The `bucket-size` specifies the length of the
time group in minutes. The first time group starts with the earliest
`timestamp`. The new aggregated layout:
FIXME: Check for a story that is about aggregations on multiple partitions.

```
type suricata.aggregated_flow = record {
  timestamp: timestamp, // maximum of timestamps
  count: count, // number of aggregated events
  pcap_cnt: count, // sum of `pcap_cnt`s
  src_ip: addr,
  dest_ip: addr,
  dest_port: port,
  proto: string,
  event_type: string, // = "aggregated_flow"
  // flow fields:
  aggregated_flow:record {
    pkts_toserver_sum: count,
    pkts_toclient_sum: count,
    bytes_toserver_sum: count,
    bytes_toclient_sum: count, // FIXME: Can it overflow?
    start_min: time,
    end_max: time,
    contains_alert: bool // if alert true if any in the group is true
  }
}
```
