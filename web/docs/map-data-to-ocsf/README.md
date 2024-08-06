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
analytics**, allowing the processes to scale indepedently.

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

### Parse the input

We first need to parse the log file into structured form that we can work with
the individual fields. Thanks to Tenzir's [Zeek
integration](../integrations/zeek.md), this is easy:

```bash
tenzir --tql2 'read_zeek_tsv' < conn.log
```

<details>
<summary>Output</summary>

```json
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

### Option A: Map from OCSF

Now that we have structured data to work with, our objective is to map the
fields from the Zeek conn.log to OCSF. The corresponding event class in OCSF
is [Network Activity](https://schema.ocsf.io/1.3.0/classes/network_activity).
We will be using OCSF v1.3.0 throughout this tutorial.

To make the mapping process more organized, we map per attribute group:

1. **Classification**: Important for the taxonomy and schema itself
2. **Occurrence**: Temporal characteristics about when the event happened
3. **Context**: Temporal characteristics about when the event happened
4. **Primary**: Defines the key semantics of the given event

Within each attribute group, we go through the attributes in the order of the
three requirement flags **required**, **recommended**, and **optional**.

This yields the following pipeline pattern:

```
read_zeek_tsv
where @name == "zeek.conn" 
this = {
  // Classification (required)
  // Classification (recommended)
  // Classification (optional)
  // Occurrence (required)
  // Occurrence (recommended)
  // Occurrence (optional)
  // Context (required)
  // Context (recommended)
  // Context (optional)
  // Primary (required)
  // Primary (recommended)
  // Primary (optional)
}
@name = "ocsf.network_activity"
```

This four pipeline operators do the following things:

1. `read_zeek_tsv`: structure the data
2. `where`: reestrict ourself to a single event type
3. `this = { ... }`: assign a new record to this event. This is where all the
   work happens next.
4. `@name = ...`: give the event a new schema name.

#### Classification Attributes

Now roll your sleeves up and dive into the first attribute group:
Classification.

Here's the TQL:

```
class_uid = 4001
activity_id = 6
activity_name = "Traffic"
this = {
  // --- Classification (required) ---
  activity_id: activity_id,
  category_uid: 4,
  class_uid: class_uid,
  type_id: class_uid * 100 + activity_id,
  severity_id: 1,
  // --- Classification (optional) ---
  activity_name: activity_name,
  category_name: "Network Activity",
  class_name: "Network Activity",
  severity: "Informational",
}
// TODO: provide a function for this and make it possible to reference
// `type_id` from the same assignment.
//type_name: ocsf_type_name(type_id),
```

Note that we need multiple assignment: three to add new values to the event that
we then use afterwards in the `type_id` computation `class_uid * 100 +
activity_id`.

#### Occurrence Attributes

Let's tackle the next group: Occurrence. These attributes are all about time. We
won't repeat the above record fields in the assignment, but the idea is to
incrementally construct a giant statement with the assignment `this = { ... }`:

```
this = {
  // --- Classification ---
  ...
  // --- Occurrence (required) ---
  time: ts,
  // --- Occurrence (recommended) ---
  // TODO: provide a function for this.
  // timezone_offset: ...
  // --- Occurrence (optional) ---
  duration: duration,
  end_time: ts + duration,
  start_time: ts,
}
```

#### Context Attributes

The Context attributes provide enhancing information. Most notably, the
`metadata` attribute holds data-source specific information and the `unmapped`
attribute collects all fields that we cannot map directly to their semantic
counterparts in OCSF.

```
this = {
  // --- Classification, Occurrence ---
  ...
  // --- Context (required) ---
  metadata: {
    log_name: "conn", // Zeek calls it "path"
    logged_time: _write_ts,
    product: {
      name: "Zeek",
      vendor_name: "Zeek",
    },
    uid: uid,
    version: "1.3.0",
  },
  // --- Context (optional) ---
  unmapped: {
    history: history,
    missed_bytes: missed_bytes,
    orig_ip_bytes: orig_ip_bytes,
    resp_ip_bytes: resp_ip_bytes,
    // TODO: should we map these to `intermediate_ips` in Network
    // Endpoint?
    tunnel_parents: tunnel_parents,
  },
}
```

#### Primary Attributes

The primary attributes define the semantics of the event class itself. This is
where the core value is, as we are mapping the most event-specific information.

For this, we still need to precompute several attributes that we are going to
use in the `this = { ... }` assignment. You can see the use of `if`/`else` here
to create a constant field based on values in the original event.

```
if local_orig and local_resp {
  direction = "Lateral"
  direction_id = 3
} else if local_orig {
  direction = "Outbound"
  direction_id = 2
} else if local_resp {
  direction = "Inbound"
  direction_id = 1
} else {
  direction = "Unknown"
  direction_id = 0
}
if proto == "tcp" {
  protocol_num = 6
} else if proto == "udp" {
  protocol_num = 17
} else if proto == "icmp" {
  protocol_num = 1
} else {
  protocol_num = -1
}
if id.orig_h.is_v6() or id.resp_h.is_v6() {
  protocol_ver_id = 6
} else {
  protocol_ver_id = 4
}
this = {
  // --- Classification, Occurrence, Context ---
  ...
  // --- Primary (required) ---
  dst_endpoint: {
    ip: id.resp_h,
    port: id.resp_p,
    // TODO: start a conversation in the OCSF Slack to figure out how to
    // assign the entire connection a protocol. We use svc_name as the
    // next best thing, but it clearly can't be different between
    // endpoints for the service semantics that Zeek has.
    svc_name: service,
  },
  // --- Primary (recommended) ---
  connection_info: {
    uid: community_id,
    direction: direction,
    direction_id: direction_id,
    protocol_ver_id: protocol_ver_id,
    protocol_name: proto,
    protocol_num: protocol_num,
  },
  src_endpoint: {
    ip: id.orig_h,
    port: id.orig_p,
    svc_name: service,
  },
  // TODO: we actually could go deeper into the `conn_state` field and
  // choose a more accurate status. But this would require string
  // manipulations and/or regex matching, which TQL doesn't have yet.
  status: "Other",
  status_code: conn_state,
  status_id: 99,
  traffic: {
    bytes_in: resp_bytes,
    bytes_out: orig_bytes,
    packets_in: resp_pkts,
    packets_out: orig_pkts,
    total_bytes: orig_bytes + resp_bytes,
    total_packets: orig_pkts + resp_pkts,
  },
  // --- Primary (optional) ---
  // TODO
  // - `ja4_fingerprint_list`: once we have some sample logs with JA4
  //   fingerprints, which requires an additional Zeek package, we should
  //   populate them here.
  // - `tls`: if we buffer ssl log for this connection, we could add the
  //   information in here.
}
```

#### Recap

Phew, that's a wrap. And we just completed one log! Here is the corresponding
OCSF event:

```json
{
  "activity_id": 6,
  "category_uid": 4,
  "class_uid": 4001,
  "metadata": {
    "log_name": "conn",
    "logged_time": null,
    "product": {
      "name": "Zeek",
      "vendor_name": "Zeek"
    },
    "uid": "C1UE8M1G6ok0h7OrJi",
    "version": "1.3.0"
  },
  "time": "2021-11-17T13:45:03.913656",
  "type_id": 400106,
  "status": "Other",
  "status_code": "OTH",
  "status_id": 99,
  "activity_name": "Traffic",
  "category_name": "Network Activity",
  "class_name": "Network Activity",
  "duration": "58.0us",
  "end_time": "2021-11-17T13:45:03.913714",
  "start_time": "2021-11-17T13:45:03.913656",
  "unmapped": {
    "history": null,
    "missed_bytes": 0,
    "orig_ip_bytes": 36,
    "resp_ip_bytes": 36,
    "tunnel_parents": null
  },
  "dst_endpoint": {
    "ip": "198.71.247.91",
    "port": 0,
    "svc_name": null
  },
  "connection_info": {
    "uid": "1:A+WZZOx8yg3UCoNV1IeiSNZUxEk=",
    "direction": "Unknown",
    "direction_id": 0,
    "protocol_ver_id": 4,
    "protocol_name": "icmp",
    "protocol_num": 1
  },
  "src_endpoint": {
    "ip": "35.87.13.125",
    "port": 8,
    "svc_name": null
  },
  "traffic": {
    "bytes_in": 8,
    "bytes_out": 8,
    "packets_in": 1,
    "packets_out": 1,
    "total_bytes": 16,
    "total_packets": 2
  }
}
```

And for completeness, here's the pipeline when stitching together the above
snippets:

<details>
<summary>Complete pipeline definition</summary>

```
read_zeek_tsv
where @name == "zeek.conn" 
class_uid = 4001
activity_id = 6
activity_name = "Traffic"
if local_orig and local_resp {
  direction = "Lateral"
  direction_id = 3
} else if local_orig {
  direction = "Outbound"
  direction_id = 2
} else if local_resp {
  direction = "Inbound"
  direction_id = 1
} else {
  direction = "Unknown"
  direction_id = 0
}
if proto == "tcp" {
  protocol_num = 6
} else if proto == "udp" {
  protocol_num = 17
} else if proto == "icmp" {
  protocol_num = 1
} else {
  protocol_num = -1
}
if id.orig_h.is_v6() or id.resp_h.is_v6() {
  protocol_ver_id = 6
} else {
  protocol_ver_id = 4
}
this = {
  // --- Classification (required) ---
  activity_id: activity_id,
  category_uid: 4,
  class_uid: class_uid,
  type_id: class_uid * 100 + activity_id,
  severity_id: 1,
  // --- Classification (optional) ---
  activity_name: activity_name,
  category_name: "Network Activity",
  class_name: "Network Activity",
  severity: "Informational",
  // TODO: provide a function for this and make it possible to reference
  // `type_id` from the same assignment.
  //type_name: ocsf_type_name(type_id),
  // --- Occurrence (required) ---
  time: ts,
  // --- Occurrence (recommended) ---
  // TODO: provide a function for this.
  // timezone_offset: ...
  // --- Occurrence (optional) ---
  duration: duration,
  end_time: ts + duration,
  start_time: ts,
  // --- Context (required) ---
  metadata: {
    log_name: "conn", // Zeek calls it "path"
    logged_time: _write_ts,
    product: {
      name: "Zeek",
      vendor_name: "Zeek",
    },
    uid: uid,
    version: "1.3.0",
  },
  // --- Context (optional) ---
  unmapped: {
    history: history,
    missed_bytes: missed_bytes,
    orig_ip_bytes: orig_ip_bytes,
    resp_ip_bytes: resp_ip_bytes,
    // TODO: should we map these to `intermediate_ips` in Network
    // Endpoint?
    tunnel_parents: tunnel_parents,
  },
  // --- Primary (required) ---
  dst_endpoint: {
    ip: id.resp_h,
    port: id.resp_p,
    // TODO: start a conversation in the OCSF Slack to figure out how to
    // assign the entire connection a protocol. We use svc_name as the
    // next best thing, but it clearly can't be different between
    // endpoints for the service semantics that Zeek has.
    svc_name: service,
  },
  // --- Primary (recommended) ---
  connection_info: {
    uid: community_id,
    direction: direction,
    direction_id: direction_id,
    protocol_ver_id: protocol_ver_id,
    protocol_name: proto,
    protocol_num: protocol_num,
  },
  src_endpoint: {
    ip: id.orig_h,
    port: id.orig_p,
    svc_name: service,
  },
  // TODO: we actually could go deeper into the `conn_state` field and
  // choose a more accurate status. But this would require string
  // manipulations and/or regex matching, which TQL doesn't have yet.
  status: "Other",
  status_code: conn_state,
  status_id: 99,
  traffic: {
    bytes_in: resp_bytes,
    bytes_out: orig_bytes,
    packets_in: resp_pkts,
    packets_out: orig_pkts,
    total_bytes: orig_bytes + resp_bytes,
    total_packets: orig_pkts + resp_pkts,
  },
  // --- Primary (optional) ---
  // TODO
  // - `ja4_fingerprint_list`: once we have some sample logs with JA4
  //   fingerprints, which requires an additional Zeek package, we should
  //   populate them here.
  // - `tls`: if we buffer ssl log for this connection, we could add the
  //   information in here.
}
@name = "ocsf.network_activity"
```

</details>

You can imagine that this process is time-consuming when doing it thoroughly, as
many data sources of dozens of unique event types that each need to be mapped
individually.

Let's cerebrate real quick what we did:

1. Start at the OCSF event class and map all attributes, partioned by group.
2. We tried to find counterparts for the attributes in the original event.
3. Everything that didn't have a counterpart ended in `unmapped`.
4. In the end, we had a single `this = { .. }` to replace the original event.

We could also use an alternative approach:

1. Consider the original event `this = {unmapped: this}`.
2. Go to the original event and map each field to its corresponding attribute.
3. Drop the mapped field from the input.
4. In the end, `unmapped` will contain the remaining attributes.

### Option B: Map to OCSF

Let's see how the alternate approach looks like. Here's a sample event again:

```json
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

As we've already establisehd the mapping from conn.log to OCSF above, let's just
write the pipeline to illustrate the different approach:

```text title="conn-to-ocsf.tql"
read_zeek_tsv
where @name == "zeek.conn"
this = {unmapped: this}
time = unmapped.ts
drop unmapped.ts
metadata.uid = unmapped.uid
drop unmapped.uid
src_endpoint = {
  ip: unmapped.id.orig_h,
  port: unmapped.id.orig_p,
  svc_name: unmapped.service,
}
dst_endpoint = {
  ip: unmapped.id.resp_h,
  port: unmapped.id.resp_p,
  svc_name: unmapped.service,
}
drop unmapped.id, unmapped.service
duration = unmapped.duration
drop unmapped.duration
if unmapped.local_orig and unmapped.local_resp {
  direction = "Lateral"
  direction_id = 3
} else if unmapped.local_orig {
  direction = "Outbound"
  direction_id = 2
} else if unmapped.local_resp {
  direction = "Inbound"
  direction_id = 1
} else {
  direction = "Unknown"
  direction_id = 0
}
drop unmapped.local_orig, unmapped.local_resp
if unmapped.proto == "tcp" {
  protocol_num = 6
} else if unmapped.proto == "udp" {
  protocol_num = 17
} else if unmapped.proto == "icmp" {
  protocol_num = 1
} else {
  protocol_num = -1
}
if unmapped.id.orig_h.is_v6() or unmapped.id.resp_h.is_v6() {
  protocol_ver_id = 6
} else {
  protocol_ver_id = 4
}
connection_info = {
  uid: unmapped.community_id,
  direction: direction,
  direction_id: direction_id,
  protocol_ver_id: protocol_ver_id,
  protocol_name: unmapped.proto,
  protocol_num: protocol_num,
}
drop unmapped.community_id, unmapped.proto
drop direction, direction_id, protocol_ver_id, protocol_num
traffic = {
  bytes_in: unmapped.resp_bytes,
  bytes_out: unmapped.orig_bytes,
  packets_in: unmapped.resp_pkts,
  packets_out: unmapped.orig_pkts,
  total_bytes: unmapped.orig_bytes + unmapped.resp_bytes,
  total_packets: unmapped.orig_pkts + unmapped.resp_pkts,
}
drop unmapped.orig_bytes, unmapped.resp_bytes
drop unmapped.orig_pkts, unmapped.resp_pkts
metadata.logged_time = unmapped._write_ts
drop unmapped._write_ts
```

Let's this pipeline:

```bash
tenzir --tql2 -f conn-to-ocsf.tql < conn.log
```

This contains the following event, among many others:

```json
{
  "unmapped": {
    "conn_state": "OTH",
    "missed_bytes": 0,
    "history": null,
    "orig_ip_bytes": 36,
    "resp_ip_bytes": 36,
    "tunnel_parents": null
  },
  "time": "2021-11-17T13:45:03.913656",
  "metadata": {
    "uid": "C1UE8M1G6ok0h7OrJi",
    "logged_time": null
  },
  "src_endpoint": {
    "ip": "35.87.13.125",
    "port": 8,
    "svc_name": null
  },
  "dst_endpoint": {
    "ip": "198.71.247.91",
    "port": 0,
    "svc_name": null
  },
  "duration": "58.0us",
  "connection_info": {
    "uid": "1:A+WZZOx8yg3UCoNV1IeiSNZUxEk=",
    "direction": "Unknown",
    "direction_id": 0,
    "protocol_ver_id": 4,
    "protocol_name": "icmp",
    "protocol_num": 1
  },
  "traffic": {
    "bytes_in": 8,
    "bytes_out": 8,
    "packets_in": 1,
    "packets_out": 1,
    "total_bytes": 16,
    "total_packets": 2
  }
}
```

We've mapped all Zeek fields to their equivalent OCSF attribute. Are we done?
No. We are still missing several attributes that are required, recommended, and
optional in the Network Activity event class. So now you must go back to the
schema documentation and identify the missing attributes.

:::tip Conclusion: Map from or to OCSF?
We went through two approaches: (A) mapping from the OCSF event class to the
original event, and vice versa, (B) starting at the original event and mapping
to the OCSF event calss. Both approaches have pros and cons:

Option A:
- ✅ Single pass over the OCSF event class.
- ✅ Easy to follow assignment of the form `this = { ... }` that describes the
  whole event.
- ❌ The `unmapped` attribute must be explicitly constructed.
- ❌ Lengthy process when original events only have a few fields.

Option (B):
- ✅ Single pass over the original event.
- ✅ The `unmapped` attribute always contains what hasn't been mapped
- ❌ Addional noise when referencing fields via `unmapped.field`.
- ❌ Must explicitly drop fields from `unmapped`.

Ultimately it's a matter of preference and use case, so it's good to know both
approaches and decide which one to pick on a case-by-case basis.
:::

### Option C: Combine Option A and B

There's a combination of Option A and Option B. We can take the idea of starting
at the OCSF schema as in Option A, but use the assign-and-drop approach from
option B.

Here's how it looks like:

```
read_zeek_tsv
where @name == "zeek.conn"
this = { x: this }
time = x.ts
metadata.uid = x.uid
metadata.logged_time = x._write_ts
src_endpoint.ip = x.id.orig_h
src_endpoint.port = x.id.orig_p
src_endpoint.svc_name = x.service
dst_endpoint.ip = x.id.resp_h
dst_endpoint.port = x.id.resp_p
dst_endpoint.svc_name = x.service
connection_info.uid = x.community_id
if x.local_orig and x.local_resp {
  connection_info.direction = "Lateral"
  connection_info.direction_id = 3
} else if x.local_orig {
  connection_info.direction = "Outbound"
  connection_info.direction_id = 2
} else if x.local_resp {
  connection_info.direction = "Inbound"
  connection_info.direction_id = 1
} else {
  connection_info.direction = "Unknown"
  connection_info.direction_id = 0
}
if x.id.orig_h.is_v6() or x.id.resp_h.is_v6() {
  connection_info.protocol_ver_id = 6
} else {
  connection_info.protocol_ver_id = 4
}
connection_info.protocol_name = x.proto
if x.proto == "tcp" {
  connection_info.protocol_num = 6
} else if x.proto == "udp" {
  connection_info.protocol_num = 17
} else if x.proto == "icmp" {
  connection_info.protocol_num = 1
} else {
  connection_info.protocol_num = -1
}
traffic.bytes_in = x.resp_bytes
traffic.bytes_out = x.orig_bytes
traffic.packets_in = x.resp_pkts
traffic.packets_out = x.orig_pkts
traffic.total_bytes = x.orig_bytes + x.resp_bytes
traffic.total_packets = x.orig_pkts + x.resp_pkts
// Drop all mapped fields.
drop (
  x.ts, x.uid, x.id, x.service, x.local_orig, x.local_resp, x.community_id,
  x.proto, x.orig_bytes, x.resp_bytes, x.orig_pkts, x.resp_pkts, x._write_ts,
)
// Move the raw event to the back and rename it to unmapped.
this = {
  ...this,
  unmapped: x,
}
drop x
@name = "ocsf.network_activity"
```

This approach has the advantage of doing a single pass over the OCSF event while
not explicitly assigning `unmapped`.

### Extend your mapping to multiple event types

So far we've mapped just a single event. But Zeek has dozens of different event
types.
