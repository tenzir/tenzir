---
description: Network traffic flow summaries
---

# NetFlow

import CommercialPlugin from '@site/presets/CommercialPlugin.md';

<CommercialPlugin/>

[NetFlow](https://en.wikipedia.org/wiki/NetFlow) is suite of protocols for
computing and relaying flow-level statistics. An *exporter*, such as a router or
switch, aggregates packets into flow records and sends them to a *collector*.

:::note Supported Versions
VAST has native support for NetFlow **v5**, **v9**, and **IPFIX**. We have [a
blog post][netflow-blog-post] about how we implement *Flexible NetFlow*. For
IPFIX we support Private Enterprise Numbers 3054 (IXIA IxFlow) and 29305
(Bidirectional Flow Export) are supported. Please contact us if you require
support for additional Private Enterprise Numbers.
[netflow-blog-post]: https://tenzir.com/blog/flexible-netflow-for-flexible-security-analytics/
:::

## Parser

VAST can either act as collector or parse binary NetFlow data on standard input.
The NetFlow version is automatically identified at runtime, and mixing multiple
versions (e.g., from multiple export devices) is possible.

To spin up a VAST client as NetFlow a collector, use the `vast import netflow`
command:

```bash
vast import -l :2055/tcp netflow
```

A commonly used NetFlow collector is `nfcapd`, which writes NetFlow
messages into framed files. To replay from `nfcapd` you can use `nfreplay`:

```bash
vast import -l :9995/udp netflow
nfreplay < path/to/capture.nfcapd # Exports all records to 127.0.0.1:9995
```

Because VAST behaves like any other UNIX tool, it can also import NetFlow
messages from files or standard input directly:

```bash
# From file
vast import -r path/to/netflow.bin netflow

# Pipe multiple files at once
cat path/to/*.bin | vast import netflow
```
