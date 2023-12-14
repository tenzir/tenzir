---
title: Contextualization Made Simple
authors: [mavam]
date: 2023-12-07
last_updated: 2023-12-12
tags: [context, enrich, node, pipelines, suricata, threat-intel, iocs]
comments: true
---

How would you create a contextualization engine? What are the essential building
blocks? We asked ourselves these questions after studying what's out there and
built from scratch a high-performance contextualization framework in Tenzir.
This blog post introduces this brand-new framework, provides usage examples, and
describes how you can build your own context plugin.

![Contextualization](contextualization.excalidraw.svg)

<!--truncate-->

This is the second post of the our contextualization series. If you haven't read
the first post, go check it out and learn [how Splunk, Elastic, and Sentinel
support contextualization](/blog/enrichment-complexity-in-the-wild).

## Requirements

After studying how others tackle the enrichment use case and talking to numerous
practitioners in the SecOps community, we went to the drawing board to identify
what we really need.

1. **Dynamic context state updates**. In security, we're especially interested
   in use cases where the enrichment context is dynamic and changes over time.
   For example, the threat landscape is often represented in the form of
   observables, IOCs, or TTPs. Their utility quickly decays over time. Many
   indicators are only useful for a couple of days, as attacker infrastructure
   can be ephemeral and change rapidly. As a result, we need the ability to
   change our context state to keep a useful representation.

2. **Decoupled context management and use**. Conceptually, a context has a write
   path to update its state, and a read path access its state. These two paths
   operate independently and its the job of the context to coordinate access to
   its shared state so that reads and writes do not conflict.

3. **Flexible notions of context type**. Most systems out there treat enrichment
   as a join that brings two tables together. But what about Bloom filters? And
   ML model inference? What about API calls? Or custom libraries that shield a
   context? We're not always enriching with just a table, but many other types
   of context. Hence we need a dedicated abstraction what constitutes a context.

A corollary of (3) is that we would like to support various *lookup modes*:

![Contextualization Modes](contextualization-modes.excalidraw.svg)

We define these lookup modes as follows:

1. **In-band**. The context data is co-located with the dataflow where it should
   act upon. This is especially important for high-velocity dataflows where
   there's a small time budget to perform an enrichment. For example, we've seen
   network monitors like [Zeek](https://zeek.org) and
   [Suricata](https://suricata.io) links produce structured logs at 250k EPS,
   which would mean that enrichment cannot take more than 4 microseconds
   per event.
2. **Out-of-band**. The context data is far away from the to-be-contextualized
   dataflow. We encounter this mode when the context is intellectual property,
   when the context state is massive and maintenance is complex, or when it's
   created on-the-fly based on request by a service. A REST API is the most
   common example.
3. **Hybrid**. When both performance matters and state is not possible to ship
   to the contextualization point itself, then a hybrid approach can be a viable
   middle ground. [Google Safe Browsing][safebrowsing] is an example of this
   kind, where the Chrome browser keeps a subset of context state that
   represents threat actor URLs in the form of partial hashes, and when a users
   visits a URL where a partial match occurs, Chrome performs a candidate check
   using an API call. More than 99% of checked URLs never make it to the remote
   API, making this approach scalable. Note that the extra layer of hashing also
   protects the privacy of the entity performing the context lookup.

[safebrowsing]: https://security.googleblog.com/2022/08/how-hash-based-safe-browsing-works-in.html

## The Tenzir Contextualization Framework

As principled engineers, we took those requirements to the drawing board and
built a solution that meets all of them. Two foundations of the Tenzir
architecture made it possible to arrive at an elegant solution that results in a
simple-yet-powerful user experience: (1) a pipeline-based data flow model, and
(2) the ability to manage state at continuously running Tenzir nodes. Let's walk
through a typical use case that explains the building blocks.

:::info Example Scenario
During a compromise assessment, the security engineer Pada Wan is tasked with
finding out whether the constituency initiates any connections to known
command-and-control servers. Pada takes the
[ThreatFox](https://threatfox.abuse.ch/) OSINT feed, a community malware where
practioners can share IOCs containing IPs, domains, URLs, and hashes. Pada's
organization uses Suricata to monitor their networks and now wants to leverage
DNS logs to identify possible lookups to known attacker infrastructure.

Pada, follwing first principles, remembers: *"Through a pipeline strong and wise,
safe the constituency will stay, hmm."*
:::

### Create a context

First create a **context** in a Tenzir node by running the following pipeline:

```
context create threatfox lookup-table
```

This yields the following output:

```json
{
  "num_entries": 0,
  "name": "threatfox"
}
```

The `context` operator manages context instances. It takes a context name and
type as positional arguments. The `lookup-table` type is a key-value mapping
where a key is used to perform the context lookup and the value can be any
structured additional data.

### Load data into the context

Next we fill the context with the contents of the ThreatFox feed. Here's the how
we query the API with a HTTP POST request:

```
from https://threatfox-api.abuse.ch/api/v1/ query=get_iocs days:=1
```

The response looks as follows:

```json
{
  "query_status": "ok",
  "data": [
    {
      "id": "1209500",
      "ioc": "8.219.229.99:4433",
      "threat_type": "botnet_cc",
      "threat_type_desc": "Indicator that identifies a botnet command&control server (C&C)",
      "ioc_type": "ip:port",
      "ioc_type_desc": "ip:port combination that is used for botnet Command&control (C&C)",
      "malware": "win.cobalt_strike",
      "malware_printable": "Cobalt Strike",
      "malware_alias": "Agentemis,BEACON,CobaltStrike,cobeacon",
      "malware_malpedia": "https://malpedia.caad.fkie.fraunhofer.de/details/win.cobalt_strike",
      "confidence_level": 80,
      "first_seen": "2023-12-04 16:00:16 UTC",
      "last_seen": null,
      "reference": null,
      "reporter": "malpulse",
      "tags": null
    },
    {
    ..
    },
    {
    ..
    }
  ]
}
```

Unfortunately the data is not yet in right shape yet. We need one IOC event per
lookup table entry, but the above is one giant event with all IOCs in the nested
`data` array. We can get to the desired shape with the
[`yield`](/next/operators/yield) operator hoists the array elements
into top-level events. Let's take a look at one of the events:

```
from https://threatfox-api.abuse.ch/api/v1/ query=get_iocs days:=1
| yield data[]
| head 1
```

```json
{
  "id": "1209500",
  "ioc": "8.219.229.99:4433",
  "threat_type": "botnet_cc",
  "threat_type_desc": "Indicator that identifies a botnet command&control server (C&C)",
  "ioc_type": "ip:port",
  "ioc_type_desc": "ip:port combination that is used for botnet Command&control (C&C)",
  "malware": "win.cobalt_strike",
  "malware_printable": "Cobalt Strike",
  "malware_alias": "Agentemis,BEACON,CobaltStrike,cobeacon",
  "malware_malpedia": "https://malpedia.caad.fkie.fraunhofer.de/details/win.cobalt_strike",
  "confidence_level": 80,
  "first_seen": "2023-12-04 16:00:16 UTC",
  "last_seen": null,
  "reference": null,
  "reporter": "malpulse",
  "tags": null
}
```

Yes, this is something the `context` update can work with. Now that the data is
in the right shape, all we need is piping it to `context update`:

```
from https://threatfox-api.abuse.ch/api/v1/ query=get_iocs days:=1
| yield data[]
| where ioc_type == "domain"
| context update threatfox --key ioc
```

This outputs:

```json
{
  "num_entries": 57,
  "name": "threatfox"
}
```

That is, 57 entries have been added successfully to the `threatfox` context.

### Enrich with the context

We've now loaded the context and can use it in other pipelines. As we're in a
compromise assessment as example, we're interested in a realtime view of the
network traffic. So we'd like to hook the feed of all flow logs streaming into a
Tenzir node. Let's say we have a Suricata `eve.json` file that we follow
continuously and import into a running node:

```
from file --follow /suricata/eve.json read suricata
| import
```

Now we hook into the DNS live feed for enrichment, keep only the matches, and
forward them to a Slack channel via [`fluent-bit`](/next/operators/fluent-bit):

```
export --live
| where #schema == "suricata.dns"
| enrich threatfox --field dns.rrname
| where threatfox.key != null
| fluent-bit slack webhook=IR_TEAM_SLACK_CHANNEL_URL
```

In more detail:
- `export --live` hooks into the import data feed at the node
- `where #schema == "suricata.dns"` restricts the feed to Suricata DNS events
- `enrich threatfox --field dns.rrname` joins the lookup table with the RR name
  of the DNS request
- `where threatfox.key != null` ignores non-matching enrichments
- `fluent-bit slack webhook=IR_TEAM_SLACK_CHANNEL_URL` sends the events to a
  Slack channel

One such matching enrichment may looks like this:

```json
{
  "timestamp": "2021-11-17T16:57:42.389824",
  "flow_id": 1542499730911936,
  "pcap_cnt": 3167,
  "vlan": null,
  "in_iface": null,
  "src_ip": "45.85.90.164",
  "src_port": 56462,
  "dest_ip": "198.71.247.91",
  "dest_port": 53,
  "proto": "UDP",
  "event_type": "dns",
  "community_id": null,
  "dns": {
    "version": null,
    "type": "query",
    "id": 1,
    "flags": null,
    "qr": null,
    "rd": null,
    "ra": null,
    "aa": null,
    "tc": null,
    "rrname": "bza.fartit.com",
    "rrtype": "RRSIG",
    "rcode": null,
    "ttl": null,
    "tx_id": 0,
    "grouped": null,
    "answers": null
  },
  "threatfox": {
    "key": "bza.fartit.com",
    "context": {
      "id": "1209087",
      "ioc": "bza.fartit.com",
      "threat_type": "payload_delivery",
      "threat_type_desc": "Indicator that identifies a malware distribution server (payload delivery)",
      "ioc_type": "domain",
      "ioc_type_desc": "Domain name that delivers a malware payload",
      "malware": "apk.irata",
      "malware_printable": "IRATA",
      "malware_alias": null,
      "malware_malpedia": "https://malpedia.caad.fkie.fraunhofer.de/details/apk.irata",
      "confidence_level": 100,
      "first_seen": "2023-12-03 14:05:20 UTC",
      "last_seen": null,
      "reference": "",
      "reporter": "onecert_ir",
      "tags": [
        "irata"
      ]
    },
    "timestamp": "2023-12-04T13:52:49.043157"
  }
}
```

Note the new field `threatfox` that is the context name. The `key` that matched
has the value `bza.fartit.com`, which is also `dns.rrname`. There's also a
`timestamp` field when the enrichment took place, and the full data that we
loaded into the context under a given key.

### Summary

Let's recap what we did:

1. Create a context via `context create` that is a lookup table.
2. Populate the context via `context update` with the ThreatFox OSINT feed.
3. Use the context via `enrich` to filter matching events.
4. Forward the enriched events to a Slack channel.

The `enrich` pipeline uses a lookup table to perform an in-band enrichment. Our
first measurements indicate that there is no noticeable performance overhead.

We can visualize this pipeline as follows:

![Contextualization Example](contextualization-example.excalidraw.svg)

## Comparison

How is this different to others, e.g., Splunk, Elastic, and Sentinel? If you
don't recall how these three work, go back to our [previous blog
post](/blog/enrichment-complexity-in-the-wild).

1. **Simplicity**. The core abstraction is incredibly simple—an opaque context
   that can be used from two sides. You can simultaneously feed the context with
   a pipeline to update its state, and use it many other places to enrich your
   dataflows.

2. **Flexibility**. The `enrich` operator gives you full control where you want
   to perform the contextualization. Place it before `import`, and it's an
   ingest-time enrichment. Put it after `export`, and it's a search-time
   enrichment. The abstraction is always same, regardless of the location.

3. **Extensibility**. This blog post showed only one context type, the lookup
   table. This covers the most common enrichment scenario. But you can implement
   your own context types. A context plugin receives the full pipeline dataflow,
   and as a developer, you get [Apache Arrow](https://arrow.apache.org) record
   batches. This columnar representation works seamlessly with many data tools.

Stay tuned for more context plugins. Up next on our roadmap are three other
in-band context types: a Bloom filter, Sigma rules, and a
[MaxMind](https://github.com/maxmind/libmaxminddb)-based GeoIP context.

You can try all of this yourself by heading over to
[app.tenzir.com](https://app.tenzir.com). Deploy a cloud-based demo node and
enrich your life. As always, we're here to help and are looking forward to
meeting you in our [Discord community](/discord).
