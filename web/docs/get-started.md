# Get Started

Tenzir is a data pipeline solution for optimizing cloud and data costs, running
detections and analytics, building a centralized security data lake, or creating
a decentralized security data fabric.

![Tenzir Moving Parts](platform-and-nodes.excalidraw.svg)

## Explore the demo node

The easiest way to get started is try it out yourself. It takes just a few
steps:

1. [Create a free account](installation/create-an-account.md) by signing in:

![Sign in](example-signin.png)

2. Go to the [Overview](https://app.tenzir.com/overview) page:

![Overview](example-overview.png)

3. Create a demo node by clicking the *Add* button in the nodes pane and select
   *Cloud-hosted demo node*:

![Add node](example-add-node.png)

5. Follow the guided tour after the node becomes available (~1 min).

6. Start [learning TQL](language.md) and explore the
   data set visually, e.g., to by running aggregations and
   [plotting data](operators/chart.md):

![Bar chart](example-barchart.png)

## A quick TQL tour

Let's run a few example pipelines by copying the below examples
and pasting them into the [Explorer](https://app.tenzir.com/explorer). We
pre-loaded the demo node with [Zeek](https://zeek.org) and
[Suricata](https://suricata.io) logs derived from the M57 dataset that we also
use in our [user guides](installation.md).

Start with:

```
export | taste
```

This pipeline uses [`export`](operators/export.md) to emit all data
stored at the demo node. We pipe the output to
[`taste`](operators/taste.md) to get a sample of 10 events per
unique schema:

<details>
<summary>Output</summary>

```json
{
  "timestamp": "2021-11-18T09:48:16.122571",
  "flow_id": 1722746302079096,
  "pcap_cnt": 349263,
  "vlan": null,
  "in_iface": null,
  "src_ip": "172.17.2.163",
  "src_port": 63342,
  "dest_ip": "45.46.53.140",
  "dest_port": 2222,
  "proto": "TCP",
  "event_type": "tls",
  "community_id": null,
  "tls": {
    "sni": null,
    "session_resumed": null,
    "subject": "C=FR, OU=Seefzjitxo Aolexzn, CN=albfyae.mobi",
    "issuerdn": "C=FR, ST=XF, L=Fke, O=Jvohtaneo Znpfkecey Eotel Aorod, CN=albfyae.mobi",
    "serial": "0A:F1",
    "fingerprint": "6b:0d:bc:a3:ec:fc:4b:56:8a:51:aa:dc:96:b3:e7:35:e6:99:3f:60",
    "ja3": {
      "hash": "51c64c77e60f3980eea90869b68c58a8",
      "string": "771,49196-49195-49200-49199-49188-49187-49192-49191-49162-49161-49172-49171-157-156-61-60-53-47-10,10-11-13-35-23-65281,29-23-24,0"
    },
    "ja3s": {
      "hash": "7c02dbae662670040c7af9bd15fb7e2f",
      "string": "771,157,65281-35"
    },
    "notbefore": "2021-09-23T09:50:44.000000",
    "notafter": "2023-09-23T14:57:14.000000",
    "version": "TLS 1.2"
  },
  "metadata": {
    "flowints": {
      "applayer.anomaly.count": null
    },
    "flowbits": [
      "ET.Evil",
      "ET.BotccIP"
    ]
  }
}
{
  "timestamp": "2021-11-18T09:32:13.566661",
  "flow_id": 1030296579147908,
  "pcap_cnt": 342987,
  "vlan": null,
  "in_iface": null,
  "src_ip": "172.17.2.163",
  "src_port": 63226,
  "dest_ip": "81.214.126.173",
  "dest_port": 2222,
  "proto": "TCP",
  "event_type": "tls",
  "community_id": null,
  "tls": {
    "sni": null,
    "session_resumed": null,
    "subject": "C=PT, OU=Pejmpse Idtyoor Geiw, CN=myzdef.biz",
    "issuerdn": "C=PT, ST=NP, L=Dejxhypqn Tyswmkejf, O=Enfdxjtlz Gucuat LLC., CN=myzdef.biz",
    "serial": "25:9F",
    "fingerprint": "89:98:69:69:01:8e:e9:a3:e6:ba:17:7a:f5:c6:e1:b8:1b:70:e8:cc",
    "ja3": {
      "hash": "51c64c77e60f3980eea90869b68c58a8",
      "string": "771,49196-49195-49200-49199-49188-49187-49192-49191-49162-49161-49172-49171-157-156-61-60-53-47-10,10-11-13-35-23-65281,29-23-24,0"
    },
    "ja3s": {
      "hash": "7c02dbae662670040c7af9bd15fb7e2f",
      "string": "771,157,65281-35"
    },
    "notbefore": "2021-09-22T17:14:19.000000",
    "notafter": "2023-09-23T01:14:49.000000",
    "version": "TLS 1.2"
  },
  "metadata": {
    "flowints": {
      "applayer.anomaly.count": null
    },
    "flowbits": null
  }
}
```

(Only first 2 results shown. Output may vary.)

:::note Demo Dataset
On this site we display the data in JSON. In the Explorer, you can enjoy a
richer display in an interactive table. You can also produce the outputs here by
invoking `tenzir <pipeline>` on the command line or
`docker run -it tenzir/tenzir <pipeline>` when using Docker.
:::

</details>

The `export` operator is a *source* that only produces data, `taste` is a
*transformation* that consumes and produces data, and there are *sinks* that
only consume data. Either you do not provide a sink (like above) and can click
the *Run* button to see the results in the app, or you provide a sink to deploy
the pipeline continuously.

Now filter the data with [`where`](operators/where.md) to only
look at Suricata alerts:

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

The above example extracts connections from the subnet 10.10.5.0/25 that either
have sent more than 1 MiB or lasted longer than 30 minutes.

Aside from filtering, you can also perform aggregations with
[`summarize`](operators/summarize.md):

```
export
| where #schema == "suricata.alert"
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

For counting field values, [`top`](operators/top.md) and
[`rare`](operators/rare.md) come in handy:

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

This was just a quick tour. The [user guides](installation.md) cover a lot more
material.

Ready to bring your data to the table? Then continue reading to learn how to
deploy your own node.

## Up next

Now that you got a first impression of Tenzir pipelines, [onboard your own
node](installation/deploy-a-node.md) and dive deeper by

- following the [user guides](installation.md) with step-by-step tutorials of
  common use cases
- learning more about the [language](language.md), [operators](operators.md),
  [connectors](connectors.md), [formats](formats.md), and the [data
  model](data-model.md)
- understanding [why](why-tenzir.md) we built Tenzir and how it compares to
  other systems

Don't forget that we're here to help! If you have any questions, swing by our
friendly [community Discord](/discord) or open a [GitHub
discussion](https://github.com/tenzir/tenzir/discussions).
