---
title: Tenzir v4.7
authors: [dominiklohmann]
date: 2023-12-19
tags: [release, grok, kv, geoip, performance]
comments: true
---

[Tenzir v4.7](https://github.com/tenzir/tenzir/releases/tag/v4.7.0) brings a new
context type, two parsers, four new operators, improvements to existing parsers,
and a sizable under-the-hood performance improvement.

![Tenzir v4.7](tenzir-v4.7.excalidraw.svg)

<!-- truncate -->

## Enrich with the GeoIP context

Use the `geoip` context to enrich events with information from a MaxMind GeoIP®
database.

To get started, [download the freely available GeoLite2 MaxMind
database](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data), or use
any other MaxMind database. We'll use the country database file
`GeoLite2-Country.mmdb`.

```text {0} title="Create a 'geoip' context named 'country'"
context create country geoip --db-path /path/to/GeoLite2-Country.mmdb
```

```text {0} title="Enrich Suricata events with the 'country' context"
export
| where #schema == /suricata.*/
/* Apply the context to both source and destination IP address fields */
| enrich src_country=country --field src_ip
| enrich dest_country=country --field dest_ip
/* Use just the country's isocode, and discard the rest of the information */
| replace src_country=src_country.context.country.iso_code,
          dest_country=dest_country.context.country.iso_code
```

```json {0} title="Possible output"
{
  "timestamp": "2021-11-17T14:02:38.165570",
  "flow_id": 1837021175481117,
  "pcap_cnt": 357,
  "vlan": null,
  "in_iface": null,
  "src_ip": "45.137.23.27",
  "src_port": 47958,
  "dest_ip": "198.71.247.91",
  "dest_port": 53,
  "proto": "UDP",
  "event_type": "dns",
  "community_id": "1:0nZC/6S/pr+IceCZ04RjDZbX+KI=",
  "dns": {
    // ...
  },
  "src_country": "NL",
  "dest_country": "US"
}
```

The `geoip` context is a powerful building block for in-band enrichments.
Besides country codes and country names you can add region codes, region names,
cities, zip codes, and geographic coordinates. With the flexibility of the
contextualization framework this information you can now get this information in
real-time.

:::info Follow our Blog Post Series
Read more about contexts in our blog post series:
1. [Enrichment Complexity in the Wild](/blog/enrichment-complexity-in-the-wild)
2. [Contextualization Made Simple](/blog/contextualization-made-simple)
:::

## Grok and KV Parsers

The [`kv`](/next/formats/kv) and [`grok`](/next/formats/grok) parsers combine
well with the [`parse`](/next/operators/parse) operator introduced with Tenzir
v4.6. The former reads key-value pairs by splitting strings based on regular
expressions, and the latter uses a parser modeled after the [Logstash
`grok` plugin][logstash-grok] in Elasticsearch.

[logstash-grok]: https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html

Parse a fictional HTTP request log with `grok`:

```json {0} title="Example input"
{
  "message": "55.3.244.1 GET /index.html 15824 0.043"
}
```

```text {0} title="Parse with grok"
parse message grok "%{IP:client} %{WORD:method} %{URIPATHPARAM:request} %{NUMBER:bytes} %{NUMBER:duration}"
```

```json {0} title="Example output"
{
  "message": {
    "client": "55.3.244.1",
    "method": "GET",
    "request": "/index.html",
    "bytes": 15824,
    "duration": 0.043
  }
}
```

Extract space-separated `key=value` pairs with `kv`:

```json {0} title="Example input"
{
  "message": "foo=1 bar=2 baz=3 qux=4"
}
```

```text {0} title="Parse with kv"
parse message kv "\s+" "="
```

```json {0} title="Example output"
{
  "message": {
    "foo": 1,
    "bar": 2,
    "baz": 3,
    "qux": 4
  }
}
```

## Slice and Dice Events

The [`slice`](/next/operators/slice) operator is a more powerful version of the
`head` and `tail` operators. It allows for selecting a contiguous range of
events given a half-closed interval.

```text {0} title="Get the second 100 events"
slice --begin 100 --end 200
```

Negative values for the interval count from the end rather than from the start:

```text {0} title="Get the last 5 events"
slice --begin -5
```

Positive and negative values can also be combined:

```text {0} title="Get everything but the first 10 and the last 10 events"
slice --begin 10 --end -10
```

## Lightweight Endpoint Snapshot

Use the [`processes`](/next/operators/processes),
[`sockets`](/next/operators/sockets), and [`nics`](/next/operators/nics) sources
to get a snapshot of running processes, sockets, and available network
interfaces, respectively.

```text {0} title="Top three running processes by name"
processes
| top name
| head 3
```

```json {0} title="Possible output"
{
  "name": "MTLCompilerService",
  "count": 24
}
{
  "name": "zsh",
  "count": 16
}
{
  "name": "VTDecoderXPCServ",
  "count": 9
}
```

## Performance Improvements

We've fixed a long-standing bug in Tenzir's pipeline execution engine that
improve performance for some operators:

1. Operators and loaders that interface with blocking third-party APIs sometimes
   delayed partial results until the next partial result arrived through the
   blocking API. This bug affected the `tcp`, `zmq`, `kafka`, and `nic` loaders
   and the `shell`, `fluent-bit`, `velociraptor`, and `python` operators. These
   loaders and operators are now generally more responsive.
2. The time-to-first-result for pipelines with many operators is now shorter,
   and the first result no longer takes an additional 20ms per operator in the
   pipeline to arrive.

## Want More?

We provide a full list of changes [in our changelog](/changelog#v470).

Head over to [app.tenzir.com](https://app.tenzir.com) to play with the new
features and join [our Discord server](/discord)—the perfect place to ask
questions, chat with Tenzir users and developers, and to discuss your feature
ideas!
