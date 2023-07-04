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
any input.

Another source is [`export`](operators/sources/export.md), which initiates a
pipeline with all data stored data at a node. We pre-loaded the demo node with
the M57 dataset that we also use in our [user guides](user-guides.md). Pipe
`export` to [`head`](operators/transformations/head.md) to get the 10 first
events in the dataflow:

```
export
| head
```

<details>
<summary>Output</summary>

TODO

</details>

Let's filter some events with [expressions](language/expressions.md) with
[`where`](operators/transformations/where.md):

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

Expressions combine rich-typed predicates with AND, OR, and NOT. Here's a more
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

For counting, the [`top`](operators/transformations/top.md) and
[`rare`](operators/transformations/rare.md) come in handy:

```
export
| where #schema == "zeek.notice"
| top msg
| head 5
'
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
material.

## Onboard your own node

Adding a own node takes just few minutes:

1. Visit the [configurator](https://app.tenzir.com/configurator) and downloading
   a configuration file.
2. Install a node using the provided instructions (also explained below).

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="universal" label="All Platforms" default>

Use our installer to perform a platform-specific installation:

```bash
curl https://get.tenzir.app | sh
```

The shell script asks you once to confirm the installation.

</TabItem>
<TabItem value="debian" label="Debian">

Download the latest [Debian package][tenzir-debian-package] and install it via
`dpkg`:

```bash
dpkg -i tenzir-static-amd64-linux.deb
```

[tenzir-debian-package]: https://github.com/tenzir/tenzir/releases/latest/download/tenzir-static-amd64-linux.deb

</TabItem>
<TabItem value="nix" label="Nix">

Try Tenzir with our `flake.nix`:

```bash
nix run github:tenzir/tenzir/stable
```

Install Tenzir by adding `github:tenzir/tenzir/stable` to your flake inputs, or
use your preferred method to include third-party modules on classic NixOS.

</TabItem>
<TabItem value="linux" label="Linux">

Download a tarball with our [static binary][tenzir-tarball] for all Linux
distributions and unpack it into `/opt/tenzir`:

```bash
tar xzf tenzir-static-x86_64-linux.tar.gz -C /
```

We also offer prebuilt statically linked binaries for every Git commit to the
`main` branch.

```bash
version="$(git describe --abbrev=10 --long --dirty --match='v[0-9]*')"
curl -fsSL "https://storage.googleapis.com/tenzir-dist-public/packages/main/tenzir-${version}-linux-static.tar.gz"
```

[tenzir-tarball]: https://github.com/tenzir/tenzir/releases/latest/download/tenzir-static-x86_64-linux.tar.gz

</TabItem>
<TabItem value="macos" label="macOS">

Please use Docker [with
Rosetta](https://levelup.gitconnected.com/docker-on-apple-silicon-mac-how-to-run-x86-containers-with-rosetta-2-4a679913a0d5)
until we offer a native package.

</TabItem>
<TabItem value="docker" label="Docker">

Pull the image:

```bash
docker pull tenzir/tenzir
```

</TabItem>
<TabItem value="docker-compose" label="Docker Compose">

After having downloaded the config file from the
[configurator](https://app.tenzir.com/configurator), run:

```bash
docker compose up
```

</TabItem>
</Tabs>

## Up Next

Now that you got a first impression of Tenzir pipelines, dive deeper by

- following the [user guides](user-guides.md) with step-by-step tutorials of
  common use cases
- learning more about the [language](language.md), [operators](operators.md),
  [connectors](connectors.md), [formats](formats.md), and the [data
  model](data-model.md)
- understanding [why](why-tenzir.md) we built Tenzir and how it compares to
  other systems

We're here to help! If you have any questions, swing by our friendly [community
Discord](/discord) or open a [GitHub
discussion](https://github.com/tenzir/tenzir/discussions).
