---
sidebar_position: 10
---

# Enrich with Threat Intel

Tenzir has a powerful [enrichment framework](../../enrichment/README.md) for
real-time contextualization. The heart of the framework is a *context—a stateful
object that can be managed and used with pipelines.

## Setup a context

Prior to enriching, you need to populate a context with data. First, let's a
create a context called `threatfox` that uses a lookup table, i.e., a key-value
mapping where a key is used to perform the lookup and the value can be any
structured additional data.

```tql
context::create_lookup_table "threatfox"
```

```tql
{num_entries: 0, name: "threatfox"}
```

After creating a context, we load data into the context. In our example, we load
data from the [ThreatFox](https://threatfox.abuse.ch/) API:

```tql
load_http "https://threatfox-api.abuse.ch/api/v1/",
  body={query: "get_iocs", days: 1}
unroll data
where data.ioc_type == "domain"
context::update "threatfox", key="ioc", value=data
```

```tql
{num_entries: 57, name: "threatfox"}
```

That is, 57 entries have been added successfully to the `threatfox` context.

<details>
<summary>Example data for context updating</summary>

If we replace `context::update` in the above pipeline with `head 5`, we get
output similar to the following, depending on the current state of the API:

```json
{
  "id": "1213056",
  "ioc": "deletefateoow.pw",
  "threat_type": "botnet_cc",
  "threat_type_desc": "Indicator that identifies a botnet command&control server (C&C)",
  "ioc_type": "domain",
  "ioc_type_desc": "Domain that is used for botnet Command&control (C&C)",
  "malware": "win.lumma",
  "malware_printable": "Lumma Stealer",
  "malware_alias": "LummaC2 Stealer",
  "malware_malpedia": "https://malpedia.caad.fkie.fraunhofer.de/details/win.lumma",
  "confidence_level": 75,
  "first_seen": "2023-12-15 15:31:00 UTC",
  "last_seen": null,
  "reference": "",
  "reporter": "stoerchl",
  "tags": [
    "LummaStealer"
  ]
}
{
  "id": "1213057",
  "ioc": "perceivedomerusp.pw",
  "threat_type": "botnet_cc",
  "threat_type_desc": "Indicator that identifies a botnet command&control server (C&C)",
  "ioc_type": "domain",
  "ioc_type_desc": "Domain that is used for botnet Command&control (C&C)",
  "malware": "win.lumma",
  "malware_printable": "Lumma Stealer",
  "malware_alias": "LummaC2 Stealer",
  "malware_malpedia": "https://malpedia.caad.fkie.fraunhofer.de/details/win.lumma",
  "confidence_level": 75,
  "first_seen": "2023-12-15 15:31:00 UTC",
  "last_seen": null,
  "reference": "",
  "reporter": "stoerchl",
  "tags": [
    "LummaStealer"
  ]
}
{
  "id": "1213058",
  "ioc": "showerreigerniop.pw",
  "threat_type": "botnet_cc",
  "threat_type_desc": "Indicator that identifies a botnet command&control server (C&C)",
  "ioc_type": "domain",
  "ioc_type_desc": "Domain that is used for botnet Command&control (C&C)",
  "malware": "win.lumma",
  "malware_printable": "Lumma Stealer",
  "malware_alias": "LummaC2 Stealer",
  "malware_malpedia": "https://malpedia.caad.fkie.fraunhofer.de/details/win.lumma",
  "confidence_level": 75,
  "first_seen": "2023-12-15 15:31:00 UTC",
  "last_seen": null,
  "reference": "",
  "reporter": "stoerchl",
  "tags": [
    "LummaStealer"
  ]
}
{
  "id": "1213059",
  "ioc": "fortunedomerussea.pw",
  "threat_type": "botnet_cc",
  "threat_type_desc": "Indicator that identifies a botnet command&control server (C&C)",
  "ioc_type": "domain",
  "ioc_type_desc": "Domain that is used for botnet Command&control (C&C)",
  "malware": "win.lumma",
  "malware_printable": "Lumma Stealer",
  "malware_alias": "LummaC2 Stealer",
  "malware_malpedia": "https://malpedia.caad.fkie.fraunhofer.de/details/win.lumma",
  "confidence_level": 75,
  "first_seen": "2023-12-15 15:31:00 UTC",
  "last_seen": null,
  "reference": "",
  "reporter": "stoerchl",
  "tags": [
    "LummaStealer"
  ]
}
{
  "id": "1213060",
  "ioc": "offerdelicateros.pw",
  "threat_type": "botnet_cc",
  "threat_type_desc": "Indicator that identifies a botnet command&control server (C&C)",
  "ioc_type": "domain",
  "ioc_type_desc": "Domain that is used for botnet Command&control (C&C)",
  "malware": "win.lumma",
  "malware_printable": "Lumma Stealer",
  "malware_alias": "LummaC2 Stealer",
  "malware_malpedia": "https://malpedia.caad.fkie.fraunhofer.de/details/win.lumma",
  "confidence_level": 75,
  "first_seen": "2023-12-15 15:31:00 UTC",
  "last_seen": null,
  "reference": "",
  "reporter": "stoerchl",
  "tags": [
    "LummaStealer"
  ]
}
```

</details>

## Enrich with a context

Now that we loaded IoCs into the context, we can enrich with it in other
pipelines. Since we previously imported only domains, we would look for fields
in the data of that type.

The following pipeline subscribes to the import feed of all data arriving at the
node via `export live=true` and applies the `threatfox` context to Suricata DNS
requests in field `dns.rrname` via
[`context::enrich`](../../tql2/operators/context/enrich.md).

```tql
export live=true
where @name == "suricata.dns"
context::enrich "threatfox", key="dns.rrname"
```

Here is a sample of an event that the above pipeline yields:

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

The sub-record `threatfox` holds the enrichment details. The field `key`
contains the matching key. The field `context` is the row from the lookup table
at key `bza.fartit.com`. The field `timestamp` is the time when the enrichment
occurred.

:::tip In-Depth Enrichment
Make sure to read our detailed [explanation of
enrichemnt](../../enrichment/README.md) to unlock the full potential of Tenzir's
contextualization framework.
:::
