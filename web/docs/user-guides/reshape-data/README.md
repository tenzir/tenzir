---
sidebar_position: 2
---

# Reshape data

Tenzir comes with numerous [transformation
operators](../../operators/transformations/README.md) that do change the the
shape of their input and produce a new output. Here is a visual overview of
transformations that you can perform over a data frame:

![Reshaping Overview](reshaping.excalidraw.svg)

We'll walk through examples for each depicted operator, using the
[M57](../../user-guides.md) dataset. All examples assume that you have imported
the M57 sample data into a node, as explaiend in the
[quickstart](../../get-started.md#quickstart). We therefore start every pipeline
with [`export`](../../operators/sources/export.md).

## Filter rows with `where`

Use [`where`](../../operators/transformations/where.md) to filter rows in the
input with an [expression](../../language/expressions.md).

Filter by metadata using the `#schema` selector:

```bash
tenzir 'export | where #schema == "suricata.alert"'
```

<details>
<summary>Output</summary>

```json
{
  "timestamp": "2021-11-17T13:52:05.695469",
  "flow_id": 1868285155318879,
  "pcap_cnt": 143,
  "vlan": null,
  "in_iface": null,
  "src_ip": "14.1.112.177",
  "src_port": 38376,
  "dest_ip": "198.71.247.91",
  "dest_port": 123,
  "proto": "UDP",
  "event_type": "alert",
  "community_id": null,
  "alert": {
    "app_proto": null,
    "action": "allowed",
    "gid": 1,
    "signature_id": 2017919,
    "rev": 2,
    "signature": "ET DOS Possible NTP DDoS Inbound Frequent Un-Authed MON_LIST Requests IMPL 0x03",
    "category": "Attempted Denial of Service",
    "severity": 2,
    "source": {
      "ip": null,
      "port": null
    },
    "target": {
      "ip": null,
      "port": null
    },
    "metadata": {
      "created_at": [
        "2014_01_03"
      ],
      "updated_at": [
        "2014_01_03"
      ]
    }
  },
  "flow": {
    "pkts_toserver": 2,
    "pkts_toclient": 0,
    "bytes_toserver": 468,
    "bytes_toclient": 0,
    "start": "2021-11-17T13:52:05.695391",
    "end": null,
    "age": null,
    "state": null,
    "reason": null,
    "alerted": null
  },
  "payload": null,
  "payload_printable": null,
  "stream": null,
  "packet": null,
  "packet_info": {
    "linktype": null
  },
  "app_proto": "failed"
}
```

(Only 1 out of 19 shown.)

</details>

Or by using type and field extractors:

```bash
tenzir 'export | where 10.10.5.0/25 && (orig_bytes > 1 Mi || duration > 30 min)'
```

<details>
<summary>Output</summary>

```json
{
  "ts": "2021-11-19T06:30:30.918301",
  "uid": "C9T8pykxdsT7iSrc9",
  "id": {
    "orig_h": "10.10.5.101",
    "orig_p": 50046,
    "resp_h": "87.120.8.190",
    "resp_p": 9090
  },
  "proto": "tcp",
  "service": null,
  "duration": "5.09m",
  "orig_bytes": 1394538,
  "resp_bytes": 95179,
  "conn_state": "S1",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": "ShADad",
  "orig_pkts": 5046,
  "orig_ip_bytes": 1596390,
  "resp_pkts": 5095,
  "resp_ip_bytes": 298983,
  "tunnel_parents": null,
  "community_id": "1:UPodR2krvvXUGhc/NEL9kejd7FA=",
  "_write_ts": null
}
{
  "ts": "2021-11-19T07:05:44.694927",
  "uid": "ChnTjeQncxZrb0ZWg",
  "id": {
    "orig_h": "10.10.5.101",
    "orig_p": 50127,
    "resp_h": "87.120.8.190",
    "resp_p": 9090
  },
  "proto": "tcp",
  "service": null,
  "duration": "54.81s",
  "orig_bytes": 1550710,
  "resp_bytes": 97122,
  "conn_state": "S1",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": "ShADadww",
  "orig_pkts": 5409,
  "orig_ip_bytes": 1767082,
  "resp_pkts": 5477,
  "resp_ip_bytes": 316206,
  "tunnel_parents": null,
  "community_id": "1:aw0CtkT7YikUZWyqdHwgLhqJXxU=",
  "_write_ts": null
}
{
  "ts": "2021-11-19T06:30:15.910850",
  "uid": "CxuTEOgWv2Z74FCG6",
  "id": {
    "orig_h": "10.10.5.101",
    "orig_p": 50041,
    "resp_h": "87.120.8.190",
    "resp_p": 9090
  },
  "proto": "tcp",
  "service": null,
  "duration": "36.48m",
  "orig_bytes": 565,
  "resp_bytes": 507,
  "conn_state": "S1",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": "ShADad",
  "orig_pkts": 78,
  "orig_ip_bytes": 3697,
  "resp_pkts": 77,
  "resp_ip_bytes": 3591,
  "tunnel_parents": null,
  "community_id": "1:r337wYxbKPDv5Vkjoz3gGuld1bs=",
  "_write_ts": null
}
```

</details>

The above example extracts connections that either have sent more than 1 MiB or
lasted longer than 30 minutes. 

:::info Extractors
Tenzir's expression language uses
[extractors](../../language/expressions.md#extractors) to locate fields of
interest.

If you don't know a field name but have concrete value, say an IP address,
you can apply a query over all schemas having fields of the `ip` type by writing
`:ip == 172.17.2.163`. The left-hand side of this predicate is a *type
extractor*, denoted by `:T` for a type `T`. The right-hand side is the IP
address literal `172.17.2.163`. You can go one step further and just write
`172.17.2.163` as query. Tenzir infers the literal type and makes a predicate
out of it, i.e.,. `x`, expands to `:T == x` where `T` is the type of `x`. Under
the hood, the predicate all possible fields with type address and yields a
logical OR.

In the above example, the value `10.10.5.0/25` actually expands to the
expression `:ip in 10.10.5.0/25 || :subnet == 10.10.5.0/25`, meaning, Tenzir
looks for any IP address field and performs a top-k prefix search, or any subnet
field where the value matches exactly.
:::

## Limit the output with `head` and `tail`

Use the [`head`](../../operators/transformations/head.md) and
[`tail`](../../operators/transformations/tail.md) operators to get the first or
last N records of the input.

:::caution `tail` is blocking
The `tail` operator must wait for its entire input, whereas `head N` terminates
immediately after the first `N` records have arrived. Use `head` for
the majority of use cases and `tail` only when you have to.
:::

## Pick fields with `select` and `drop`

Use the [`select`](../../operators/transformations/select.md) operator to
restrict the output to a list of fields.

```bash
tenzir '
  export
  | where #schema == "suricata.alert"
  | select src_ip, dest_ip, severity, signature
  | head 3
  '
```

<details>
<summary>Output</summary>

```json
{
  "src_ip": "8.218.64.104",
  "dest_ip": "198.71.247.91",
  "alert": {
    "signature": "SURICATA UDPv4 invalid checksum",
    "severity": 3
  }
}
{
  "src_ip": "14.1.112.177",
  "dest_ip": "198.71.247.91",
  "alert": {
    "signature": "ET DOS Possible NTP DDoS Inbound Frequent Un-Authed MON_LIST Requests IMPL 0x03",
    "severity": 2
  }
}
{
  "src_ip": "167.94.138.20",
  "dest_ip": "198.71.247.91",
  "alert": {
    "signature": "SURICATA UDPv4 invalid checksum",
    "severity": 3
  }
}
```

Note that `select` does not reorder the input fields. Use
[`put`](../../operators/transformations/put.md) for adjusting the field order.

</details>

## Sample schemas with `taste`

TODO

## Add fields with `put` and `extend`

TODO

## Exchange fields with `replace`

TODO

## Give fields new names with `rename`

TODO

## Aggreate records with `summarize`

Use [`summarize`](../../operators/transformations/summarize.md) to group and
aggregate data.

```bash
tenzir '
  export
  | #schema == "suricata.alert"
  | summarize count=count(src_ip) by severity
  '
```

<details>
<summary>Output</summary>

```json
{"alert.severity": 1, "count": 134644}
{"alert.severity": 2, "count": 26780}
{"alert.severity": 3, "count": 179713}
```

</details>

Suricata alerts with lower severity are more critical, with severity 1 being the
highest. Let's group by alert signature containing the substring `SHELLCODE`:

```bash
tenzir '
  export
  | where severity == 1
  | summarize count=count(src_ip) by signature
  | where /.*SHELLCODE.*/
  '
```

<details>
<summary>Output</summary>

```json
{"alert.signature": "ET SHELLCODE Possible Call with No Offset TCP Shellcode", "count": 2}
{"alert.signature": "ET SHELLCODE Possible %41%41%41%41 Heap Spray Attempt", "count": 32}
```

</details>

## Reorder records with `sort`

Use [`sort`](../../operators/transformations/sort.md) to arrange the output
records according to the order of a specific field.

```bash
tenzir '
  export
  | #schema == "suricata.alert"
  | summarize count=count(src_ip) by severity
  | sort count desc
  '
```

```json
{"alert.severity": 3, "count": 179713}
{"alert.severity": 1, "count": 134644}
{"alert.severity": 2, "count": 26780}
```

## Deduplicate with `unique`

Use [`unique`](../../operators/transformations/unique.md) to remove adjacent
duplicates. Typically, this comes up after a `sort`.

TODO
