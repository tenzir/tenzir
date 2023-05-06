# netflow

Reads NetFlow v5, v9, and IPFIX.

## Synopsis

```
netflow
```

## Description

import CommunityEdition from '@site/presets/CommunityEdition.md';

<CommunityEdition/>

[NetFlow](https://en.wikipedia.org/wiki/NetFlow) is suite of protocols for
computing and relaying flow-level statistics. An *exporter*, such as a router or
switch, aggregates packets into flow records and sends them to a *collector*.

VAST supports NetFlow v5, v9, and IPFIX via *Flexible NetFlow*. For IPFIX we
support Private Enterprise Numbers 3054 (IXIA IxFlow) and 29305 (Bidirectional
Flow Export). Please contact us if you require support for additional Private
Enterprise Numbers.

The parser auto-detects the NetFlow version at runtime, so you don't have to
provide a specific version.

## Examples

Read a binary NetFlow file:

```
from file /tmp/netflow.bin read netflow
```

Become a NetFlow collector at port 9995:

```
from udp -l :9995 read netflow
```

Replay a `nfcapd` file via `nfreplay` into the above pipeline:

```bash
# Exports all records to 127.0.0.1:9995
nfreplay < path/to/capture.nfcapd
```
