---
sidebar_position: 11
---

# Enrich with Network Inventory

Tenzir's contextualization framework features [lookup
tables](../../contexts/lookup-table.md) that you can use to
[`enrich`](../../operators/enrich.md) your pipelines. Lookup tables have one
unique property that makes them attractive for tracking information associated
with CIDR subnets: when you use `subnet` values as keys, you can probe the
lookup table with `ip` values and will get a longest-prefix match.

To illustrate, consider this lookup table:

```
10.0.0.0/22 → Machines
10.0.0.0/24 → Servers
10.0.1.0/24 → Clients
```

When you have subnets as keys as above, you can query them with an IP address
during enrichment. Say you want to enrich IP address `10.0.0.1`. Since the
longest (bitwise) prefix match is `10.0.0.0/24`, you will get `Servers` as a
result. The same goes for IP address `10.0.0.255`, but `10.0.1.1` will yield
`Clients`. The IP address `10.0.2.1` yields Machines, since it is neither in
`10.0.0.0/24` nor `10.0.1.0/24`, but `10.0.0.0/21`. The IP adress `10.0.4.1`
won't match at all, because it's not any of the three subnets.

## Populate subnet mappings from a CSV file

It's common to have Excel sheets or exported CSV files of inventory data. Let's
consider this example:

```csv title=inventory.csv
subnet,owner,function
10.0.0.0/22,John,machines
10.0.0.0/24,Derek,servers
10.0.1.0/24,Peter,clients
```

First, create the context:

```
context create subnets lookup-table
```

Then populate it:

```
from inventory.csv
| context update subnets --key subnet
```

## Enrich IP addresses with the subnet table

Now that we have a lookup table with subnet keys, we can
[`enrich`](../../operators/enrich.md) any data containing IP addresses with it.
For example, let's consider this simplified Suricata flow record:

```json title=sample.json
{
  "timestamp": "2021-11-17T13:32:43.237882",
  "src_ip": "10.0.0.1",
  "src_port": 54402,
  "dest_ip": "10.1.1.254",
  "dest_port": 53,
  "proto": "UDP",
  "event_type": "flow",
  "app_proto": "dns",
}
```

Let's use the `enrich` operator to add the subnet context to all IP address
fields:

```
from /tmp/sample.json
| enrich subnets --field :ip
```

This yields the following output:

```json
{
  "timestamp": "2021-11-17T13:32:43.237882",
  "src_ip": "10.0.0.1",
  "src_port": 54402,
  "dest_ip": "10.1.1.254",
  "dest_port": 53,
  "proto": "UDP",
  "event_type": "flow",
  "app_proto": "dns",
  "subnets": {
    "src_ip": {
      "value": "10.0.0.1",
      "timestamp": "2024-05-29T13:02:56.368882",
      "mode": "enrich",
      "context": {
        "subnet": "10.0.0.0/24",
        "owner": "Derek",
        "function": "servers"
      }
    },
    "dest_ip": {
      "value": "10.1.1.254",
      "timestamp": "2024-05-29T13:02:56.368882",
      "mode": "enrich",
      "context": null
    }
  }
}
```

We have enriched all IP addresses in the flow event with the `subnets` context.
Now go hunt down Derek!
