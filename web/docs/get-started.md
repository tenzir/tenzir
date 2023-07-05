# Get Started

:::info What is Tenzir?
Tenzir is a distributed platform for processing and storing security event data
in a pipeline dataflow model.
:::

Dive right in at [app.tenzir.com](https://app.tenzir.com) and sign up for a free
account. The instructions below explain how to get started in just a few
minutes.

## Create a free account

You need a Tenzir account to interact with your nodes in the browser.

1. Go to [app.tenzir.com](https://app.tenzir.com)
2. Sign in with your identity provider or create an account

No strings attached: you can always delete your account via *Account* → *Delete
Account*.

## Explore the demo node

Let's run a few example [pipelines](language/pipelines.md). Every account comes
with a pre-installed demo node, but you ultimately want to [bring your
own](setup-guides/deploy-a-node/README.md). Follow along by copying the below
examples and pasting them into the [Explorer](https://app.tenzir.com/explorer).

:::tip Explorer vs. Documentation
On this site we display the data in JSON. In the Explorer, you can enjoy a
richer display in an interactive table. You can also produce the outputs here by
invoking `tenzir <pipeline>` on the [command line](command-line.md) or
`docker run -it tenzir/tenzir <pipeline>` when using Docker.
:::

Our first first pipeline produces just a single event: the version of the
Tenzir node:

```
version
```

<details>
<summary>Output</summary>

```json
{
  "version": "v4.0.0-rc2-34-g9197f7355e",
  "plugins": [
    {
      "name": "compaction",
      "version": "bundled"
    },
    {
      "name": "inventory",
      "version": "bundled"
    },
    {
      "name": "kafka",
      "version": "bundled"
    },
    {
      "name": "matcher",
      "version": "bundled"
    },
    {
      "name": "netflow",
      "version": "bundled"
    },
    {
      "name": "parquet",
      "version": "bundled"
    },
    {
      "name": "pcap",
      "version": "bundled"
    },
    {
      "name": "pipeline-manager",
      "version": "bundled"
    },
    {
      "name": "platform",
      "version": "bundled"
    },
    {
      "name": "web",
      "version": "bundled"
    }
  ]
}
```

(Output may vary based on your actual version.)

</details>

The [`version`](operators/sources/version.md) operator is a
[source](operators/sources/README.md), i.e., it outputs data but doesn't have
any input. Another source is [`export`](operators/sources/export.md), which
begins a pipeline with all stored data at a node. Pipe `export` to
[`head`](operators/transformations/head.md) to retrieve 10 events:

```
export | head
```

<details>
<summary>Output</summary>

```json
{
  "timestamp": "2021-11-18T08:23:45.304758",
  "flow_id": 1851826916903734,
  "pcap_cnt": 54742,
  "vlan": null,
  "in_iface": null,
  "src_ip": "8.249.125.254",
  "src_port": 80,
  "dest_ip": "10.6.2.101",
  "dest_port": 49789,
  "proto": "TCP",
  "event_type": "fileinfo",
  "community_id": null,
  "fileinfo": {
    "filename": "/d/msdownload/update/software/defu/2021/06/am_delta_patch_1.339.1962.0_5e6a00734b4809bcfd493118754d0ea1cd64798e.exe",
    "magic": null,
    "gaps": false,
    "state": "CLOSED",
    "md5": null,
    "sha1": null,
    "sha256": null,
    "stored": false,
    "file_id": null,
    "size": 2,
    "tx_id": 0,
    "start": 0,
    "end": 1
  },
  "http": {
    "hostname": "au.download.windowsupdate.com",
    "url": "/d/msdownload/update/software/defu/2021/06/am_delta_patch_1.339.1962.0_5e6a00734b4809bcfd493118754d0ea1cd64798e.exe",
    "http_port": null,
    "http_user_agent": "Microsoft-Delivery-Optimization/10.0",
    "http_content_type": "application/octet-stream",
    "http_method": "GET",
    "http_refer": null,
    "protocol": "HTTP/1.1",
    "status": 206,
    "redirect": null,
    "length": 2,
    "xff": null,
    "content_range": {
      "raw": "bytes 0-1/360888",
      "start": 0,
      "end": 1,
      "size": 360888
    }
  },
  "app_proto": "http",
  "metadata": {
    "flowints": {
      "http.anomaly.count": null,
      "tcp.retransmission.count": null
    },
    "flowbits": [
      "ET.INFO.WindowsUpdate",
      "exe.no.referer"
    ]
  }
}
```

(Only 1 out of 10 shown.)

</details>

:::note Demo Dataset
We pre-loaded the demo node in the app with [Zeek](https://zeek.org) and
[Suricata](https://suricata.io) logs derived from the M57 dataset. We also use
that dataset in our [user guides](user-guides.md).
:::

Let's filter out some Suricata alerts with the
[`where`](operators/transformations/where.md) operator:

```
export
| where #schema == "suricata.alert"
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

The `where` operator takes an [expression](language/expressions.md) as argument,
which combines rich-typed predicates with AND, OR, and NOT. Here's a more
elaborate example:

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

The above example extracts connections that either have sent more than 1 MiB or
lasted longer than 30 minutes.

Aside from filtering, you can also perform aggregations with
[`summarize`](operators/transformations/summarize.md):

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

For counting of field values, the [`top`](operators/transformations/top.md) and
[`rare`](operators/transformations/rare.md) come in handy:

```
export
| where #schema == "zeek.notice"
| top msg
| head 5
```

<details>
<summary>Output</summary>

```json
{"msg": "SSL certificate validation failed with (certificate has expired)", "n": 2201}
{"msg": "SSL certificate validation failed with (unable to get local issuer certificate)", "n": 1600}
{"msg": "SSL certificate validation failed with (self signed certificate)", "n": 603}
{"msg": "Detected SMB::FILE_WRITE to admin file share '\\\\10.5.26.4\\C$\\WINDOWS\\h48l10jxplwhq9eowyecjmwg0nxwu72zblns1l3v3c6uu6p6069r4c4c5yjwv_e7.exe'", "n": 339}
{"msg": "SSL certificate validation failed with (certificate is not yet valid)", "n": 324}
```

</details>

This was just a quick tour. The [user guides](user-guides.md) cover a lot more
material. Next, we'll explain how to deploy a node so that you can work with
your own data.

## Onboard your own node

Adding a node takes just few minutes:

1. Visit the [configurator](https://app.tenzir.com/configurator) 
2. Download a configuration file for your node.
3. Install your node by follow the [deployment
   instructions](setup-guides/deploy-a-node/README.md).

## Up Next

Now that you got a first impression of Tenzir pipelines, and perhaps already
a node of your own, dive deeper by

- following the [user guides](user-guides.md) with step-by-step tutorials of
  common use cases
- learning more about the [language](language.md), [operators](operators.md),
  [connectors](connectors.md), [formats](formats.md), and the [data
  model](data-model.md)
- understanding [why](why-tenzir.md) we built Tenzir and how it compares to
  other systems

Don't forget that we're here to help! If you have any questions, swing by our
friendly [community Discord](/discord) or open a [GitHub
discussion](https://github.com/tenzir/tenzir/discussions).
