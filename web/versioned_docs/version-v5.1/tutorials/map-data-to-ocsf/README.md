# Map Data to OCSF

In this tutorial you'll learn how to map events to [Open Cybersecurity Schema
Framework (OCSF)](https://schema.ocsf.io). We walk you through an example of
events from a network monitor and show how you can use Tenzir pipelines to
easily transform them so that they become OCSF-compliant events.

![OCSF Pipeline](ocsf-pipeline.svg)

The diagram above illustrates the data lifecycle and the OCSF mapping takes
place: you collect data from various data sources, each of which has a different
shape, and then convert them to a standardized representation. The primary
benefit is that **normalization decouples data acquisition from downstream
analytics**, allowing the processes to scale independently.

## OCSF Primer

The OCSF is a vendor-agnostic event schema (aka. "taxonomy") that defines
structure and semantics for security events. Here are some key terms you need to
know to map events:

- **Attribute**: a unique identifier for a specific type, e.g., `parent_folder`
  of type `String` or `observables` of type `Observable Array`.
- **Event Class**: the description of an event defined in terms of attributes,
  e.g., `HTTP Activity` and `Detection Finding`.
- **Category**: a group of event classes, e.g., `System Activity` or `Findings`.

The diagram below illustrates how subsets of attributes form an event class:

![OCSF Event Classes](ocsf-event-classes.svg)

The **Base Event Class** is a special event class that's part of every event
class. Think of it as a mixin of attributes that get automatically added:

![OCSF Base Event Class](ocsf-base-event-class.svg)

For this tutorial, we look at OCSF from the perspective of the *mapper* persona,
i.e., as someone who converts existing events into the OCSF schema. OCSF also
defines three other personas, author, producer, and analyst. These are out of
scope. Our mission as mapper is now to study the event semantics of the data
source we want to map, and translate the event to the appropriate OCSF event
class.

## Case Study: Zeek Logs

Let's map some [Zeek](https://zeek.org) logs to OCSF!

:::info What is Zeek?
The [Zeek](https://zeek.org) network monitor turns raw network traffic into
detailed, structured logs. The logs range across the OSI stack from link layer
activity to application-specific messages. In addition, Zeek provides a powerful
scripting language to act on network events, making it a versatile tool for
writing network-based detections to raise alerts.
:::

Zeek generates logs in tab-separated values (TSV) or JSON format. Here's an
example of a connection log in TSV format:

```text title="conn.log (TSV)"
#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	conn
#open	2014-05-23-18-02-04
#fields	ts	uid	id.orig_h	id.orig_p	id.resp_h	id.resp_p	proto	service	duration	orig_bytes	resp_bytes	conn_state	local_orig	missed_bytes	history	orig_pkts	orig_ip_bytes	resp_pkts	resp_ip_bytes	tunnel_parents
#types	time	string	addr	port	addr	port	enum	string	interval	count	count	string	bool	count	string	count	count	count	count	table[string]
1637155966.565338	C5luJD1ATrGDOcouW2	89.248.165.145	43831	198.71.247.91	52806	-	-	tcp	-	-	-	-	S0	-	-	0	S	1	40	0	0	-	-	-	64:9e:f3:be:db:66	00:16:3c:f1:fd:6d	GB	-	-	51.4964	-0.1224	US	VA	Ashburn	39.0469	-77.4903	1:c/CLmyk4xRElyzleEMhJ4Baf4Gk=
1637156308.073703	C5VoWQz890uMGQ80i	50.18.18.14	8	198.71.247.91	0	-	-	icmp	-	0.000049	8	8	OTH	-	-	0	-	1	36	1	36	-	-	-	64:9e:f3:be:db:66	00:16:3c:f1:fd:6d	US	CA	San Jose	37.3388	-121.8916	US	VA	Ashburn	39.0469	-77.4903	1:g6Ic2EGNte5The4ZgH83QyBviYM=
1637156441.248995	CoMNrWaF6TCc5v9F	198.71.247.91	53590	91.189.89.199	123	-	-	udp	ntp	0.132333	48	48	SF	-	-	0	Dd	1	76	1	76	-	-	-	00:16:3c:f1:fd:6d	64:9e:f3:be:db:66	US	VA	Ashburn	39.0469	-77.4903	GB	ENG	London	51.5095	-0.0955	1:+5Mf6akMTGuI1eAX/a9DaKSLPa8=
1637156614.649079	Co2oVs4STLavNDKQwf	54.176.143.72	8	198.71.247.91	0	-	-	icmp	-	0.000131	8	8	OTH	-	-	0	-	1	36	1	36	-	-	-	64:9e:f3:be:db:66	00:16:3c:f1:fd:6d	US	CA	San Jose	37.3388	-121.8916	US	VA	Ashburn	39.0469	-77.4903	1:WHH+8OuOygRPi50vrH45p9WwgA4=
1637156703.913656	C1UE8M1G6ok0h7OrJi	35.87.13.125	8	198.71.247.91	0	-	-	icmp	-	0.000058	8	8	OTH	-	-	0	-	1	36	1	36	-	-	-	64:9e:f3:be:db:66	00:16:3c:f1:fd:6d	US	OR	Boardman	45.8234	-119.7257	US	VA	Ashburn	39.0469	-77.4903	1:A+WZZOx8yg3UCoNV1IeiSNZUxEk=
```

### Step 1: Parse the input

We first need to parse the log file into structured form that we can work with
the individual fields. Thanks to Tenzir's [Zeek
support](../../integrations/zeek/README.md), we can get quickly turn TSV logs
into structured data using the
[`read_zeek_tsv`](../../tql2/operators/read_zeek_tsv.md) operator:

```bash
tenzir 'read_zeek_tsv' < conn.log
```

<details>
<summary>Output</summary>

```tql
{
  "ts": "2021-11-17T13:32:43.237881",
  "uid": "CZwqhx3td8eTfCSwJb",
  "id": {
    "orig_h": "128.14.134.170",
    "orig_p": 57468,
    "resp_h": "198.71.247.91",
    "resp_p": 80
  },
  "proto": "tcp",
  "service": "http",
  "duration": "5.16s",
  "orig_bytes": 205,
  "resp_bytes": 278,
  "conn_state": "SF",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": "ShADadfF",
  "orig_pkts": 6,
  "orig_ip_bytes": 525,
  "resp_pkts": 5,
  "resp_ip_bytes": 546,
  "tunnel_parents": null,
  "community_id": "1:YXWfTYEyYLKVv5Ge4WqijUnKTrM=",
  "_write_ts": null
}
{
  "ts": "2021-11-17T13:38:28.073703",
  "uid": "C5VoWQz890uMGQ80i",
  "id": {
    "orig_h": "50.18.18.14",
    "orig_p": 8,
    "resp_h": "198.71.247.91",
    "resp_p": 0
  },
  "proto": "icmp",
  "service": null,
  "duration": "49.0us",
  "orig_bytes": 8,
  "resp_bytes": 8,
  "conn_state": "OTH",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": null,
  "orig_pkts": 1,
  "orig_ip_bytes": 36,
  "resp_pkts": 1,
  "resp_ip_bytes": 36,
  "tunnel_parents": null,
  "community_id": "1:g6Ic2EGNte5The4ZgH83QyBviYM=",
  "_write_ts": null
}
{
  "ts": "2021-11-17T13:40:41.248995",
  "uid": "CoMNrWaF6TCc5v9F",
  "id": {
    "orig_h": "198.71.247.91",
    "orig_p": 53590,
    "resp_h": "91.189.89.199",
    "resp_p": 123
  },
  "proto": "udp",
  "service": "ntp",
  "duration": "132.33ms",
  "orig_bytes": 48,
  "resp_bytes": 48,
  "conn_state": "SF",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": "Dd",
  "orig_pkts": 1,
  "orig_ip_bytes": 76,
  "resp_pkts": 1,
  "resp_ip_bytes": 76,
  "tunnel_parents": null,
  "community_id": "1:+5Mf6akMTGuI1eAX/a9DaKSLPa8=",
  "_write_ts": null
}
{
  "ts": "2021-11-17T13:43:34.649079",
  "uid": "Co2oVs4STLavNDKQwf",
  "id": {
    "orig_h": "54.176.143.72",
    "orig_p": 8,
    "resp_h": "198.71.247.91",
    "resp_p": 0
  },
  "proto": "icmp",
  "service": null,
  "duration": "131.0us",
  "orig_bytes": 8,
  "resp_bytes": 8,
  "conn_state": "OTH",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": null,
  "orig_pkts": 1,
  "orig_ip_bytes": 36,
  "resp_pkts": 1,
  "resp_ip_bytes": 36,
  "tunnel_parents": null,
  "community_id": "1:WHH+8OuOygRPi50vrH45p9WwgA4=",
  "_write_ts": null
}
{
  "ts": "2021-11-17T13:45:03.913656",
  "uid": "C1UE8M1G6ok0h7OrJi",
  "id": {
    "orig_h": "35.87.13.125",
    "orig_p": 8,
    "resp_h": "198.71.247.91",
    "resp_p": 0
  },
  "proto": "icmp",
  "service": null,
  "duration": "58.0us",
  "orig_bytes": 8,
  "resp_bytes": 8,
  "conn_state": "OTH",
  "local_orig": null,
  "local_resp": null,
  "missed_bytes": 0,
  "history": null,
  "orig_pkts": 1,
  "orig_ip_bytes": 36,
  "resp_pkts": 1,
  "resp_ip_bytes": 36,
  "tunnel_parents": null,
  "community_id": "1:A+WZZOx8yg3UCoNV1IeiSNZUxEk=",
  "_write_ts": null
}
```

</details>

### Step 2: Map to OCSF

Now that we have structured data to work with, our objective is to map the
fields from the Zeek conn.log to OCSF. The corresponding event class in OCSF
is [Network Activity](https://schema.ocsf.io/1.3.0/classes/network_activity).
We will be using OCSF v1.3.0 throughout this tutorial.

To make the mapping process more organized, we map per attribute group:

1. **Classification**: Important for the taxonomy and schema itself
2. **Occurrence**: Temporal characteristics about when the event happened
3. **Context**: Auxiliary information about the event
4. **Primary**: Defines the key semantics of the given event

Mapping now involves going through the original `conn.log` and finding the
corresponding attribute. We recommend structuring your pipeline so that
attributes of the same group belong together.

Here's a template for the mapping pipeline:

```tql
// (1) Move original event into dedicated field that we pull our values from. We
// recommend naming the field so that it represents the respective data source.
this = { zeek: this }

// (2) Populate the OCSF event. We use comments for the OCSF attribute group to
// provide a bit of structure for the reader.
// === Classification ===
ocsf.activity_id: activity_id,
...
// === Occurrence ===
ocsf.time = zeek.ts
drop zeek.ts // <--------------- drop fields immediately after mapping
...
// === Context ===
ocsf.metadata = {
  log_name: "conn.log",
  ...
}
// === Primary ===
ocsf.src_endpoint = {
  ip: zeek.id.orig_h,
  port: zeek.id.orig_p,
  ...
}
drop zeek.id

// (3) Make all nested fields in the `ocsf` record the new top-level, and keep
// all unmapped fields in the `unmapped` attribute.
this = {...ocsf, unmapped: zeek}

// (4) Assign a new schema name to the transformed event.
@name = "ocsf.network_activity"
```

Let's unpack this:

1. With `this = { zeek: this }` we move the original event into the field
   `zeek`. This also has the benefit that we avoid name clashes when creating
   new fields in the next steps. Because we are mapping Zeek logs, we chose a
   matching record name to make the subsequent mappings almost self-explanatory.
2. The main work takes place here. Our approach is structured: for every field
   in the source event, (1) map it, and (2) remove it (via `drop`). For better
   categorization of the entire OCSF mapping, we group the mappings by OCSF
   attribute group.
3. The assignment `this = {...ocsf, unmapped: zeek}` means that we move all
   fields from the `ocsf` record into the top-level record (`this`), and at the
   same time add a new field `unmapped` that contains everything that we didn't
   map. This is a safe approach, because if we forget to map a field, it simply
   lands in the bag of unmapped stuff and will show up there later.
4. We give the event a new schema name so that we can easily filter by its shape
   in further pipelines.

Now that we know the general structure, let's get our hands dirty and go deep
into the actual mapping.

#### Classification Attributes

The classification attributes are important for the schema. Mapping them is
pretty mechanical and mostly involves going through the schema docs.

```tql
// === Classification ===
ocsf.activity_id = 6
ocsf.activity_name = "Traffic"
ocsf.category_uid = 4
ocsf.category_name = "Network Activity"
ocsf.class_uid = 4001
ocsf.class_name = "Network Activity"
ocsf.severity_id = 1
ocsf.severity = "Informational"
ocsf.type_uid = ocsf.class_uid * 100 + ocsf.activity_id
```

Note that computing the field `type_uid` involves trivial arithmetic.

#### Occurrence Attributes

Let's tackle the next group: Occurrence. These attributes are all about time.

```tql
// === Occurrence ===
ocsf.time = zeek.ts
drop zeek.ts
ocsf.duration = zeek.duration
drop zeek.duration
ocsf.end_time = ocsf.time + ocsf.duration
ocsf.start_time = ocsf.time
```

Using `+` with a value of type `time` and `duration` yields a new `time` value,
just as you'd expect.

#### Context Attributes

The Context attributes provide enhancing information. Most notably, the
`metadata` attribute holds data-source specific information. Even though
`unmapped` is part of this group, we deal it with at the very end.

```tql
// === Context ===
ocsf.metadata = {
  log_name: "conn.log",
  logged_time: zeek._write_ts,
  product: {
    name: "Zeek",
    vendor_name: "Zeek",
    cpe_name: "cpe:2.3:a:zeek:zeek",
  },
  uid: zeek.uid,
  version: "1.4.0",
}
drop zeek._path, zeek._write_ts, zeek.uid
ocsf.app_name = zeek.service
drop zeek.service
```

#### Primary Attributes

The primary attributes define the semantics of the event class itself. This is
where the core value of the data is, as we are mapping the most event-specific
information.

```tql
// === Primary ===
ocsf.src_endpoint = {
  ip: zeek.id.orig_h,
  port: zeek.id.orig_p,
}
ocsf.dst_endpoint = {
  ip: zeek.id.resp_h,
  port: zeek.id.resp_p,
}
let $proto_nums = {
  tcp: 6,
  udp: 17,
  icmp: 1,
  icmpv6: 58,
  ipv6: 41,
}
ocsf.connection_info = {
  community_uid: zeek.community_id,
  protocol_name: zeek.proto,
  protocol_num: $proto_nums[zeek.proto].otherwise(-1),
}
if zeek.id.orig_h.is_v6() or zeek.id.resp_h.is_v6() {
  ocsf.connection_info.protocol_ver_id = 6
} else {
  ocsf.connection_info.protocol_ver_id = 4
}
drop zeek.id
if zeek.local_orig and zeek.local_resp {
  ocsf.connection_info.direction = "Lateral"
  ocsf.connection_info.direction_id = 3
} else if zeek.local_orig {
  ocsf.connection_info.direction = "Outbound"
  ocsf.connection_info.direction_id = 2
} else if zeek.local_resp {
  ocsf.connection_info.direction = "Inbound"
  ocsf.connection_info.direction_id = 1
} else {
  ocsf.connection_info.direction = "Unknown"
  ocsf.connection_info.direction_id = 0
}
drop zeek.community_id, zeek.proto, zeek.local_orig, zeek.local_resp,
ocsf.status = "Other"
ocsf.status_code = zeek.conn_state
drop zeek.conn_state
ocsf.status_id = 99
ocsf.traffic = {
  bytes_in: zeek.resp_bytes,
  bytes_out: zeek.orig_bytes,
  packets_in: zeek.resp_pkts,
  packets_out: zeek.orig_pkts,
  total_bytes: zeek.orig_bytes + zeek.resp_bytes,
  total_packets: zeek.orig_pkts + zeek.resp_pkts,
}
drop zeek.resp_bytes, zeek.orig_bytes, zeek.resp_pkts, zeek.orig_pkts
```

There's a lot more going on here:

- The expression `$proto_nums[zeek.proto].otherwise(-1)` takes the value of the
  `proto` field (e.g., `tcp`) and uses it as index into a static record
  `$proto_nums`. Since we encounter weird values that we didn't account for, the
  `otherwise(-1)` makes sure we always fall back to something OCSF-compliant.
- To check whether we have an IPv4 or an IPv6 connection, we call
  `zeek.id.orig_h.is_v6()` on the IPs of the connection record. TQL comes with
  numerous [functions](../../tql2/functions.md) that make mapping a breeze.

#### Recap

Phew, that's a wrap. Here's the entire pipeline in a single piece:

<details>
<summary>Complete pipeline definition</summary>

```tql title="conn-to-ocsf.tql"
// tql2
let $proto_nums = {
  tcp: 6,
  udp: 17,
  icmp: 1,
  icmpv6: 58,
  ipv6: 41,
}
read_zeek_tsv
where @name == "zeek.conn"
this = { zeek: this }
// === Classification ===
ocsf.activity_id = 6
ocsf.activity_name = "Traffic"
ocsf.category_uid = 4
ocsf.category_name = "Network Activity"
ocsf.class_uid = 4001
ocsf.class_name = "Network Activity"
ocsf.severity_id = 1
ocsf.severity = "Informational"
ocsf.type_uid = ocsf.class_uid * 100 + ocsf.activity_id
// === Occurrence ===
ocsf.time = zeek.ts
drop zeek.ts
ocsf.duration = zeek.duration
drop zeek.duration
ocsf.end_time = ocsf.time + ocsf.duration
ocsf.start_time = ocsf.time
// === Context ===
ocsf.metadata = {
  log_name: "conn.log",
  logged_time: zeek._write_ts,
  product: {
    name: "Zeek",
    vendor_name: "Zeek",
    cpe_name: "cpe:2.3:a:zeek:zeek",
  },
  uid: zeek.uid,
  version: "1.4.0",
}
drop zeek._write_ts, zeek.uid
ocsf.app_name = zeek.service
drop zeek.service
// === Primary ===
ocsf.src_endpoint = {
  ip: zeek.id.orig_h,
  port: zeek.id.orig_p,
}
ocsf.dst_endpoint = {
  ip: zeek.id.resp_h,
  port: zeek.id.resp_p,
}
ocsf.connection_info = {
  community_uid: zeek.community_id,
  protocol_name: zeek.proto,
  protocol_num: $proto_nums[zeek.proto].otherwise(-1),
}
if zeek.id.orig_h.is_v6() or zeek.id.resp_h.is_v6() {
  ocsf.connection_info.protocol_ver_id = 6
} else {
  ocsf.connection_info.protocol_ver_id = 4
}
drop zeek.id
if zeek.local_orig and zeek.local_resp {
  ocsf.connection_info.direction = "Lateral"
  ocsf.connection_info.direction_id = 3
} else if zeek.local_orig {
  ocsf.connection_info.direction = "Outbound"
  ocsf.connection_info.direction_id = 2
} else if zeek.local_resp {
  ocsf.connection_info.direction = "Inbound"
  ocsf.connection_info.direction_id = 1
} else {
  ocsf.connection_info.direction = "Unknown"
  ocsf.connection_info.direction_id = 0
}
drop zeek.community_id, zeek.proto, zeek.local_orig, zeek.local_resp
ocsf.status = "Other"
ocsf.status_code = zeek.conn_state
drop zeek.conn_state
ocsf.status_id = 99
ocsf.traffic = {
  bytes_in: zeek.resp_bytes,
  bytes_out: zeek.orig_bytes,
  packets_in: zeek.resp_pkts,
  packets_out: zeek.orig_pkts,
  total_bytes: zeek.orig_bytes + zeek.resp_bytes,
  total_packets: zeek.orig_pkts + zeek.resp_pkts,
}
drop zeek.resp_bytes, zeek.orig_bytes, zeek.resp_pkts, zeek.orig_pkts
this = {...ocsf, unmapped: zeek}
@name = "ocsf.network_activity"
```

</details>

Let's run the pipeline:

```bash
tenzir -f conn-to-ocsf.tql < conn.log
```

You should get the following output:

```tql
{
  activity_id: 6,
  activity_name: "Traffic",
  category_uid: 4,
  category_name: "Network Activity",
  class_uid: 4001,
  class_name: "Network Activity",
  severity_id: 1,
  severity: "Informational",
  type_uid: 400106,
  time: 2021-11-17T13:45:03.913656064Z,
  duration: 58us,
  end_time: 2021-11-17T13:45:03.913714064Z,
  start_time: 2021-11-17T13:45:03.913656064Z,
  metadata: {
    log_name: "conn.log",
    logged_time: null,
    product: {
      name: "Zeek",
      vendor_name: "Zeek",
      cpe_name: "cpe:2.3:a:zeek:zeek",
    },
    uid: "C1UE8M1G6ok0h7OrJi",
    version: "1.4.0",
  },
  app_name: null,
  src_endpoint: {
    ip: 35.87.13.125,
    port: 8,
  },
  dst_endpoint: {
    ip: 198.71.247.91,
    port: 0,
  },
  connection_info: {
    community_uid: "1:A+WZZOx8yg3UCoNV1IeiSNZUxEk=",
    protocol_name: "icmp",
    protocol_num: 1,
    protocol_ver_id: 4,
    direction: "Unknown",
    direction_id: 0,
  },
  status: "Other",
  status_code: "OTH",
  status_id: 99,
  traffic: {
    bytes_in: 8,
    bytes_out: 8,
    packets_in: 1,
    packets_out: 1,
    total_bytes: 16,
    total_packets: 2,
  },
  unmapped: {
    missed_bytes: 0,
    history: null,
    orig_ip_bytes: 36,
    resp_ip_bytes: 36,
    tunnel_parents: null,
  },
}
```

### Step 3: Combine multiple pipelines

So far we've mapped just a single event. But Zeek has dozens of different event
types, and we need to write one mapping pipeline for each. But how do we combine
the individual pipelines?

Tenzir's answer for this is topic-based publish-subscribe. The
[`publish`](../../tql2/operators/publish.md) and
[`subscribe`](../../tql2/operators/subscribe.md) operators send events to, and
read events from a topic, respectively. Here's an illustration of the conceptual
approach we are going to use:

![Pub/Sub Appraoch](ocsf-pub-sub.svg)

The first pipeline publishes to the `zeek` topic:

```tql
read_zeek_tsv
publish "zeek"
```

Then we have one pipeline per Zeek event type `X` that publishes to the `ocsf`
topic:

```tql
subscribe "zeek"
where @name == "zeek.X"
// map to OCSF
publish "ocsf"
```

The idea is that all Zeek logs arrive at the topic `zeek`, and all mapping
pipelines start there by subscribing to the same topic. Each pipeline filters
out one event type. Finally, all mapping pipelines publish to the `ocsf`
topic that represents the combined feed of all OCSF events.

You can then use the same filtering pattern as with Zeek to get a subset of the
OCSF stream, e.g., `subscribe "ocsf" | where @name == "ocsf.authentication"` for
all OCSF Authentication events.

:::tip Isn't this inefficient?
You may think that copying the full feed of the `zeek` topic to every mapping
pipeline is inefficient. The good news is that it is not, for two reasons:

1. Data transfers between `publish` and `subscribe` use the same zero-copy
   mechanism that pipelines use internally for sharing of events.
2. Pipelines of the form `subscribe ... | where <predicate>` push perform
   *predicate pushdown* and send `predicate` upstream so that the filtering
   can happen as early as possible.
:::

## Summary

In this tutorial, we demonstrated how you map logs to OCSF event classes. We
used the Zeek network monitor as a case study to illustrate the general mapping
pattern. Finally, we explained how to use Tenzir's pub-sub mechanism to scale
from on to many pipelines, each of which handle a specific OCSF event class.
