---
sidebar_position: 1
---

# Shape data

Tenzir comes with numerous transformation [operators](../../operators.md) that
do change the the shape of their input and produce a new output. Here is a
visual overview of transformations that you can perform over a data frame:

![Reshaping Overview](reshaping.excalidraw.svg)

We'll walk through examples for each depicted operator, using the
[M57](../../installation.md) dataset. All examples assume that you have imported
the M57 sample data into a node, as explained in the
[quickstart](../../get-started.md#quickstart). We therefore start every pipeline
with [`export`](../../operators/export.md).

## Filter rows with `where`

Use [`where`](../../operators/where.md) to filter rows in the
input with an [expression](../../language/expressions.md).

Filter by metadata using the `#schema` selector:

```
export | where #schema == "suricata.alert"
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

```
export
| where 10.10.5.0/25 && (orig_bytes > 1 Mi || duration > 30 min)
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

The above example extracts connections from the subnet 10.10.5.0/25 that either
have sent more than 1 MiB or lasted longer than 30 minutes.

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

Use the [`head`](../../operators/head.md) and
[`tail`](../../operators/tail.md) operators to get the first or
last N records of the input.

The first 3 Zeek logs with IPs in 10.10.5.0/25:

```
export
| where #schema == /zeek.*/ && 10.10.5.0/25
| head 3
```

<details>
<summary>Output</summary>

```json
{
  "ts": "2021-11-19T04:28:06.186626",
  "cause": "violation",
  "analyzer_kind": "protocol",
  "analyzer_name": "GSSAPI",
  "uid": "CaHAWI2k6vB6BEOh65",
  "fuid": null,
  "id.orig_h": "10.10.5.101",
  "id.orig_p": 49847,
  "id.resp_h": "10.10.5.5",
  "id.resp_p": 49667,
  "id.vlan": null,
  "id.vlan_inner": null,
  "failure_reason": "Binpac exception: binpac exception: out_of_bound: ASN1EncodingMeta:more_len: 129 > 74",
  "failure_data": null
}
{
  "ts": "2021-11-19T04:28:06.186853",
  "cause": "violation",
  "analyzer_kind": "protocol",
  "analyzer_name": "GSSAPI",
  "uid": "CaHAWI2k6vB6BEOh65",
  "fuid": null,
  "id.orig_h": "10.10.5.101",
  "id.orig_p": 49847,
  "id.resp_h": "10.10.5.5",
  "id.resp_p": 49667,
  "id.vlan": null,
  "id.vlan_inner": null,
  "failure_reason": "Binpac exception: binpac exception: out_of_bound: ASN1EncodingMeta:more_len: 129 > 74",
  "failure_data": null
}
{
  "ts": "2021-11-19T04:28:06.187119",
  "cause": "violation",
  "analyzer_kind": "protocol",
  "analyzer_name": "GSSAPI",
  "uid": "CaHAWI2k6vB6BEOh65",
  "fuid": null,
  "id.orig_h": "10.10.5.101",
  "id.orig_p": 49847,
  "id.resp_h": "10.10.5.5",
  "id.resp_p": 49667,
  "id.vlan": null,
  "id.vlan_inner": null,
  "failure_reason": "Binpac exception: binpac exception: out_of_bound: ASN1EncodingMeta:more_len: 129 > 74",
  "failure_data": null
}
```

</details>

:::caution `tail` is blocking
The `tail` operator must wait for its entire input, whereas `head N` terminates
immediately after the first `N` records have arrived. Use `head` for
the majority of use cases and `tail` only when you have to.
:::

## Pick fields with `select` and `drop`

Use the [`select`](../../operators/select.md) operator to
restrict the output to a list of fields.

```
export
| where #schema == "suricata.alert"
| select src_ip, dest_ip, severity, signature
| head 3
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

</details>

Note that `select` does not reorder the input fields. Use
[`put`](../../operators/put.md) for adjusting the field order.

## Sample schemas with `taste`

The [`taste`](../../operators/taste.md) operator provides a
sample of the first N events of every unique schemas in the dataflow. For
example, to get 5 unique samples:

```
export
| taste 1
| head 5
```

<details>
<summary>Output</summary>

```json
{
  "ts": "2021-11-17T13:54:01.721755",
  "cause": "violation",
  "analyzer_kind": "protocol",
  "analyzer_name": "HTTP",
  "uid": "Cqp7rtziLijlnrxYf",
  "fuid": null,
  "id.orig_h": "87.251.64.137",
  "id.orig_p": 64078,
  "id.resp_h": "198.71.247.91",
  "id.resp_p": 80,
  "id.vlan": null,
  "id.vlan_inner": null,
  "failure_reason": "not a http request line",
  "failure_data": null
}
{
  "ts": "2021-11-17T13:33:53.748229",
  "ts_delta": "1.18m",
  "peer": "zeek",
  "gaps": 0,
  "acks": 2,
  "percent_lost": 0.0,
  "_write_ts": null
}
{
  "ts": "2021-11-17T13:32:46.565337",
  "uid": "C5luJD1ATrGDOcouW2",
  "id": {
    "orig_h": "89.248.165.145",
    "orig_p": 43831,
    "resp_h": "198.71.247.91",
    "resp_p": 52806
  },
  "proto": "tcp",
  "service": null,
  "duration": null,
  "orig_bytes": null,
  "resp_bytes": null,
  "conn_state": "S0",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": "S",
  "orig_pkts": 1,
  "orig_ip_bytes": 40,
  "resp_pkts": 0,
  "resp_ip_bytes": 0,
  "tunnel_parents": null,
  "community_id": "1:c/CLmyk4xRElyzleEMhJ4Baf4Gk=",
  "_write_ts": null
}
{
  "ts": "2021-11-18T08:05:09.134638",
  "uid": "Cwk5in34AvxJ8MurDh",
  "id": {
    "orig_h": "10.2.9.133",
    "orig_p": 49768,
    "resp_h": "10.2.9.9",
    "resp_p": 135
  },
  "rtt": "254.0us",
  "named_pipe": "135",
  "endpoint": "epmapper",
  "operation": "ept_map",
  "_write_ts": null
}
{
  "ts": "2021-11-18T08:00:21.486539",
  "uids": [
    "C4fKs01p1bdzLWvtQa"
  ],
  "client_addr": "192.168.1.102",
  "server_addr": "192.168.1.1",
  "mac": "00:0b:db:63:58:a6",
  "host_name": "m57-jo",
  "client_fqdn": "m57-jo.",
  "domain": "m57.biz",
  "requested_addr": null,
  "assigned_addr": "192.168.1.102",
  "lease_time": "59.4m",
  "client_message": null,
  "server_message": null,
  "msg_types": [
    "REQUEST",
    "ACK"
  ],
  "duration": "163.82ms",
  "trans_id": null,
  "_write_ts": null
}
```

</details>

## Add fields with `put` and `extend`

The [`extend`](../../operators/extend.md) operator appends new
fields to the input. The [`put`](../../operators/put.md)
operator does the same but drops all non-referenced fields.

Here is an example that generates host pairs plus service for Zeek connection
records. Think of the output is a the edges in graph, with the last column being
the edge type.

```
export 
| where #schema == "zeek.conn" && 10.10.5.0/25
| put id.orig_h, id.resp_h, service
| head
```

<details>
<summary>Output</summary>

```json
{
  "id.orig_h": "10.10.5.101",
  "id.resp_h": "77.75.230.91",
  "service": "http"
}
{
  "id.orig_h": "10.10.5.101",
  "id.resp_h": "10.10.5.5",
  "service": "dns"
}
{
  "id.orig_h": "10.10.5.101",
  "id.resp_h": "10.10.5.5",
  "service": "dns"
}
{
  "id.orig_h": "10.10.5.101",
  "id.resp_h": "20.189.173.1",
  "service": null
}
{
  "id.orig_h": "10.10.5.101",
  "id.resp_h": "20.189.173.1",
  "service": "ssl"
}
{
  "id.orig_h": "10.10.5.101",
  "id.resp_h": "52.109.8.21",
  "service": "ssl"
}
{
  "id.orig_h": "10.10.5.101",
  "id.resp_h": "10.10.5.5",
  "service": "dns"
}
{
  "id.orig_h": "10.10.5.101",
  "id.resp_h": "10.10.5.5",
  "service": "dns"
}
{
  "id.orig_h": "10.10.5.101",
  "id.resp_h": "20.54.88.152",
  "service": "ssl"
}
{
  "id.orig_h": "10.10.5.101",
  "id.resp_h": "13.107.42.16",
  "service": "ssl"
}
```

</details>

## Give schemas and fields new names with `rename`

The [`rename`](../../operators/rename.md) operator changes field
or schema names.

For example, rename the schema name and only print that afterwards:

```
export
| where #schema == "zeek.conn"
| rename flow=:zeek.conn
| put schema=#schema
| head 1
```

<details>
<summary>Output</summary>

```json
{
  "schema": "flow"
}
```

</details>

Rename a field:

```
export
| where #schema == "zeek.conn"
| rename src=id.orig_h, dst=id.resp_h
| put src, dst
| head
```

<details>
<summary>Output</summary>

```json
{"src": "89.248.165.145", "dst": "198.71.247.91"}
{"src": "128.14.134.170", "dst": "198.71.247.91"}
{"src": "60.205.181.213", "dst": "198.71.247.91"}
{"src": "31.44.185.120", "dst": "198.71.247.91"}
{"src": "91.223.67.180", "dst": "198.71.247.91"}
{"src": "185.73.126.70", "dst": "198.71.247.91"}
{"src": "183.136.225.42", "dst": "198.71.247.91"}
{"src": "71.6.135.131", "dst": "198.71.247.91"}
{"src": "172.104.138.223", "dst": "198.71.247.91"}
{"src": "185.94.111.1", "dst": "198.71.247.91"}
```

</details>

## Aggreate records with `summarize`

Use [`summarize`](../../operators/summarize.md) to group and
aggregate data.

```
export
| #schema == "suricata.alert"
| summarize count=count(src_ip) by severity
```

<details>
<summary>Output</summary>

```json
{
  "alert.severity": 1,
  "count": 134644
}
{
  "alert.severity": 2,
  "count": 26780
}
{
  "alert.severity": 3,
  "count": 179713
}
```

</details>

Suricata alerts with lower severity are more critical, with severity 1 being the
highest. Let's group by alert signature containing the substring `SHELLCODE`:

```
export
| where severity == 1
| summarize count=count(src_ip) by signature
| where /.*SHELLCODE.*/
```

<details>
<summary>Output</summary>

```json
{
  "alert.signature": "ET SHELLCODE Possible Call with No Offset TCP Shellcode",
  "count": 2
}
{
  "alert.signature": "ET SHELLCODE Possible %41%41%41%41 Heap Spray Attempt",
  "count": 32
}
```

</details>

## Reorder records with `sort`

Use [`sort`](../../operators/sort.md) to arrange the output
records according to the order of a specific field.

```
export
| #schema == "suricata.alert"
| summarize count=count(src_ip) by severity
| sort count desc
```

<details>
<summary>Output</summary>

```json
{
  "alert.severity": 3,
  "count": 179713
}
{
  "alert.severity": 1,
  "count": 134644
}
{
  "alert.severity": 2,
  "count": 26780
}
```

</details>

## Deduplicate with `unique`

Use [`unique`](../../operators/unique.md) to remove adjacent
duplicates. This operator comes in handy after a
[`sort`](../../operators/sort.md) that arranges the input so
that duplicates lay next to each other:

```
export
| where #schema == "zeek.kerberos"
| put client
| sort client
| unique
| head
```

<details>
<summary>Output</summary>

```json
{
  "client": "/NM"
}
{
  "client": "Administrator/EAGLEFREAKS"
}
{
  "client": "DEKSTOP-D9UMVWL$/SIMONSAYSGO.NET"
}
{
  "client": "DEKSTOP-VVCWQF5$/POLICYBARONS.COM"
}
{
  "client": "DESKTOP-1-PC$/MAXSUGER.COM"
}
{
  "client": "DESKTOP-1O7QAEA$/VICTORYPUNK.COM"
}
{
  "client": "DESKTOP-2P2S7WR$/VICTORYPUNK.COM"
}
{
  "client": "DESKTOP-30CQ14B$/FIRGREENTECH.COM"
}
{
  "client": "DESKTOP-3KI6Y6G$/JIGGEDYJACK.COM"
}
{
  "client": "DESKTOP-41SH6EJ$/DUCKKISSMIXER.COM"
}
```

</details>

To compute a unique list of values per group, use the `distinct` aggregation
function in [`summarize`](../../operators/summarize.md):

```
export
| where #schema == "zeek.conn"
| summarize sources=distinct(id.orig_h) by id.resp_h
| rename destination=id.resp_h
| head 3
```

<details>
<summary>Output</summary>

```json
{
  "destination": "192.168.201.13",
  "sources": [
    "10.12.14.101",
    "10.12.17.101"
  ]
}
{
  "destination": "192.168.62.104",
  "sources": [
    "10.12.14.101",
    "10.12.17.101"
  ]
}
{
  "destination": "10.0.177.137",
  "sources": [
    "10.7.5.133"
  ]
}
```

</details>

## Profile the pipeline with `measure`

Use [`measure`](../../operators/measure.md) to profile the input
and replace it with runtime statistics.

For example, one way to compute a histogram over the entire persisted dataset is
to perform a full scan, replace the input with statistics, and then aggregate
them by schema:

```
export
| measure
| summarize events=sum(events) by schema
| sort events desc
```

<details>
<summary>Output</summary>

```
{
  "schema": "suricata.flow",
  "events": 1129992
}
{
  "schema": "zeek.conn",
  "events": 583838
}
{
  "schema": "suricata.alert",
  "events": 341137
}
{
  "schema": "suricata.dns",
  "events": 289117
}
{
  "schema": "suricata.http",
  "events": 150736
}
{
  "schema": "zeek.dns",
  "events": 90013
}
{
  "schema": "suricata.tls",
  "events": 84608
}
{
  "schema": "zeek.http",
  "events": 75290
}
{
  "schema": "zeek.telemetry",
  "events": 72853
}
{
  "schema": "suricata.smb",
  "events": 67943
}
{
  "schema": "zeek.ssl",
  "events": 42389
}
{
  "schema": "suricata.fileinfo",
  "events": 35968
}
{
  "schema": "suricata.dcerpc",
  "events": 33055
}
{
  "schema": "zeek.files",
  "events": 21922
}
{
  "schema": "zeek.dce_rpc",
  "events": 19585
}
{
  "schema": "zeek.analyzer",
  "events": 14755
}
{
  "schema": "suricata.anomaly",
  "events": 8535
}
{
  "schema": "zeek.notice",
  "events": 5871
}
{
  "schema": "suricata.smtp",
  "events": 5208
}
{
  "schema": "zeek.weird",
  "events": 4617
}
{
  "schema": "zeek.reporter",
  "events": 3528
}
{
  "schema": "suricata.krb5",
  "events": 3378
}
{
  "schema": "zeek.ocsp",
  "events": 2874
}
{
  "schema": "zeek.kerberos",
  "events": 2708
}
{
  "schema": "zeek.x509",
  "events": 2379
}
{
  "schema": "zeek.smtp",
  "events": 1967
}
{
  "schema": "zeek.smb_mapping",
  "events": 1584
}
{
  "schema": "zeek.stats",
  "events": 1409
}
{
  "schema": "zeek.ntp",
  "events": 1224
}
{
  "schema": "zeek.smb_files",
  "events": 1140
}
{
  "schema": "suricata.ftp",
  "events": 954
}
{
  "schema": "suricata.sip",
  "events": 936
}
{
  "schema": "zeek.dpd",
  "events": 926
}
{
  "schema": "suricata.dhcp",
  "events": 648
}
{
  "schema": "zeek.tunnel",
  "events": 606
}
{
  "schema": "zeek.sip",
  "events": 565
}
{
  "schema": "zeek.loaded_scripts",
  "events": 512
}
{
  "schema": "zeek.capture_loss",
  "events": 476
}
{
  "schema": "zeek.ntlm",
  "events": 429
}
{
  "schema": "zeek.pe",
  "events": 315
}
{
  "schema": "suricata.snmp",
  "events": 288
}
{
  "schema": "zeek.dhcp",
  "events": 267
}
{
  "schema": "zeek.snmp",
  "events": 132
}
{
  "schema": "suricata.tftp",
  "events": 62
}
{
  "schema": "suricata.stats",
  "events": 12
}
{
  "schema": "zeek.traceroute",
  "events": 9
}
{
  "schema": "zeek.ftp",
  "events": 4
}
{
  "schema": "suricata.ikev2",
  "events": 2
}
{
  "schema": "suricata.ftp_data",
  "events": 1
}
{
  "schema": "zeek.packet_filter",
  "events": 1
}
{
  "schema": "zeek.radius",
  "events": 1
}
```

</details>

The above pipeline performs a full scan over the data at the node. Tenzir's
pipeline optimizer pushes down predicates to avoid scans when possible. Consider
this pipeline:

```
export
| where *.id.orig_h in 10.0.0.0/8
```

The optimizer coalesces the `export` and `where` operators such that
[expression](../../language/expressions.md) `*.id.orig_h in 10.0.0.0/8` gets
pushed down to the index and storage layer.
