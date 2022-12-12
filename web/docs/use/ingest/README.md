# Ingest

Sending data to VAST (aka *ingesting*) involves spinning up a VAST client
that parses and ships the data to a [VAST server](/docs/use/run):

![Ingest process](/img/ingest.light.png#gh-light-mode-only)
![Ingest process](/img/ingest.dark.png#gh-dark-mode-only)

VAST first acquires data through a *carrier* that represents the data transport
medium. This typically involves I/O and has the effect of slicing the data into
chunks of bytes. Thereafter, the *format* determines how to parse the bytes into
structured events. On the VAST server, a partition builder (1) creates
sketches for accelerating querying, and (2) creates a *store* instance by
transforming the in-memory Arrow representation into an on-disk format, e.g.,
Parquet.

Loading and parsing take place in a separate VAST client to facilitate
horizontal scaling. The `import` command creates a client for precisly this
task.

At the server, there exists one partition builder per schema. After a
partition builder has reached a maximum number of events or reached a timeout,
it sends the partition to the catalog to register it.

:::note Lakehouse Architecture
VAST uses open standards for data in motion ([Arrow](https://arrow.apache.org))
and data at rest ([Parquet](https://parquet.apache.org/)). You only ETL data
once to a destination of your choice. In that sense, VAST resembles a [lakehouse
architecture][lakehouse-paper]. Think of the above pipeline as a chain of
independently operating microservices, each of which can be scaled
independently. The [actor
model](/docs/understand/architecture/actor-model/) architecture of
VAST enables this naturally.
[lakehouse-paper]: http://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf
:::

The following discussion assumes that you [set up a VAST
server](/docs/use/run) listening at `localhost:42000`.

## Choose an import format

The *format* defines the encoding of data. ASCII formats include JSON, CSV, or
tool-specific data encodings like Zeek TSV. Examples for binary formats are
PCAP and NetFlow.

The `import` command reads data from file or standard input and takes a
concrete format as sub-command:

```bash
vast import [options] <format> [options] [expr]
```

For example, to import a file in JSON, use the `json` format:

```bash
vast import json < data.json
```

To see a list of available import formats, run `vast import help`. To see the
help for a specific format, run `vast import <format> help`.

### JSON

The `json` import format consumes [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON) objects
according to a specified schema. That is, one line corresponds to one event. The
object field names correspond to record field names.

JSON can express only a subset VAST's data model. For example, VAST has
first-class support for IP addresses but they are strings in JSON. To get the
most out of your data and retain domain semantics, [define a schema for your
JSON objects](#provide-a-schema-for-unknown-types).

Consider the this example JSON file `data.json`:

```json
{"ts":"2011-08-15T03:36:16.748830Z","uid":"CKmcUPexVMCAAkl6h","id.orig_h":"210.87.254.81","id.orig_p":3,"id.resp_h":"147.32.84.165","id.resp_p":1,"proto":"icmp","conn_state":"OTH","missed_bytes":0,"orig_pkts":1,"orig_ip_bytes":56,"resp_pkts":0,"resp_ip_bytes":0,"tunnel_parents":[]}
{"ts":"2011-08-15T03:37:11.992151Z","uid":"CTluup1eVngpaS6e2i","id.orig_h":"147.32.84.165","id.orig_p":3923,"id.resp_h":"218.108.143.87","id.resp_p":22,"proto":"tcp","duration":3.006088,"orig_bytes":0,"resp_bytes":0,"conn_state":"S0","missed_bytes":0,"history":"S","orig_pkts":4,"orig_ip_bytes":192,"resp_pkts":0,"resp_ip_bytes":0,"tunnel_parents":[]}
{"ts":"2011-08-15T03:37:12.593013Z","uid":"C4KKBn3pbBOEm8XWOk","id.orig_h":"147.32.84.165","id.orig_p":3924,"id.resp_h":"218.108.189.111","id.resp_p":22,"proto":"tcp","duration":3.005948,"orig_bytes":0,"resp_bytes":0,"conn_state":"S0","missed_bytes":0,"history":"S","orig_pkts":4,"orig_ip_bytes":192,"resp_pkts":0,"resp_ip_bytes":0,"tunnel_parents":[]}
```

Import this file by specifying the schema `zeek.conn` that ships with VAST:

```bash
vast import --type=zeek.conn json < data.json
```

Passing a schema type via `--type` is necessary because the NDJSON objects are
just a collection of fields. VAST cannot know how to name the corresponding
table without an external hint. See the section on [mapping events to
schemas](#map-events-to-schemas) for details.

### CSV

The `import csv` command imports [comma-separated
values (CSV)](https://en.wikipedia.org/wiki/Comma-separated_values) in tabular
form. The first line in a CSV file must contain a header that describes the
field names. The remaining lines contain concrete values. Except for the header,
one line corresponds to one event.

Ingesting CSV is similar to [ingesting JSON](#JSON). It is also necessary to
[select a layout](#map-events-to-schemas) via `--type` whose field names
correspond to the CSV header field names.

For a real-world example of ingesting CSV, take a look a the section covering
[argus](#argus) below.

### Zeek

The `import zeek` command consumes [Zeek](https://zeek.org) logs in
tab-separated value (TSV) style, and the `import zeek-json` command consumes
Zeek logs as [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON) objects
as produced by the
[json-streaming-logs](https://github.com/corelight/json-streaming-logs) package.
Unlike stock Zeek JSON logs, where one file contains exactly one log type, the
streaming format contains different log event types multiplexed in a single
stream and uses an additional `_path` field to disambiguate the log type. For
stock Zeek JSON logs, use the existing `import json` with the `--type` option to
specify the log type. You do not need to specify a type for Zeek TSV, because
VAST can infer the type from `#path` comment.

Here's an example of a typical Zeek `conn.log` in TSV form:

```
#separator \x09
#set_separator  ,
#empty_field  (empty)
#unset_field  -
#path conn
#open 2014-05-23-18-02-04
#fields ts  uid id.orig_h id.orig_p id.resp_h id.resp_p proto service duration  …orig_bytes resp_bytes  conn_state  local_orig  missed_bytes  history orig_pkts …orig_ip_bytes  resp_pkts resp_ip_bytes tunnel_parents
#types  time  string  addr  port  addr  port  enum  string  interval  count coun…t  string  bool  count string  count count count count table[string]
1258531221.486539 Pii6cUUq1v4 192.168.1.102 68  192.168.1.1 67  udp - 0.163820  …301  300 SF  - 0 Dd  1 329 1 328 (empty)
1258531680.237254 nkCxlvNN8pi 192.168.1.103 137 192.168.1.255 137 udp dns 3.7801…25 350 0 S0  - 0 D 7 546 0 0 (empty)
1258531693.816224 9VdICMMnxQ7 192.168.1.102 137 192.168.1.255 137 udp dns 3.7486…47 350 0 S0  - 0 D 7 546 0 0 (empty)
1258531635.800933 bEgBnkI31Vf 192.168.1.103 138 192.168.1.255 138 udp - 46.72538…0  560 0 S0  - 0 D 3 644 0 0 (empty)
1258531693.825212 Ol4qkvXOksc 192.168.1.102 138 192.168.1.255 138 udp - 2.248589…  348  0 S0  - 0 D 2 404 0 0 (empty)
1258531803.872834 kmnBNBtl96d 192.168.1.104 137 192.168.1.255 137 udp dns 3.7488…93 350 0 S0  - 0 D 7 546 0 0 (empty)
1258531747.077012 CFIX6YVTFp2 192.168.1.104 138 192.168.1.255 138 udp - 59.05289…8  549 0 S0  - 0 D 3 633 0 0 (empty)
1258531924.321413 KlF6tbPUSQ1 192.168.1.103 68  192.168.1.1 67  udp - 0.044779  …303  300 SF  - 0 Dd  1 331 1 328 (empty)
1258531939.613071 tP3DM6npTdj 192.168.1.102 138 192.168.1.255 138 udp - - - - S0…  -  0 D 1 229 0 0 (empty)
1258532046.693816 Jb4jIDToo77 192.168.1.104 68  192.168.1.1 67  udp - 0.002103  …311  300 SF  - 0 Dd  1 339 1 328 (empty)
1258532143.457078 xvWLhxgUmj5 192.168.1.102 1170  192.168.1.1 53  udp dns 0.0685…11 36  215 SF  - 0 Dd  1 64  1 243 (empty)
1258532203.657268 feNcvrZfDbf 192.168.1.104 1174  192.168.1.1 53  udp dns 0.1709…62 36  215 SF  - 0 Dd  1 64  1 243 (empty)
1258532331.365294 aLsTcZJHAwa 192.168.1.1 5353  224.0.0.251 5353  udp dns 0.1003…81 273 0 S0  - 0 D 2 329 0 0 (empty)
```

You can import this log as follows:

```bash
vast import zeek < conn.log
```

When Zeek [rotates
logs](https://docs.zeek.org/en/stable/frameworks/logging.html#rotation), it
produces compressed batches of `*.tar.gz` regularly. If log freshness is not a
priority, you could trigger an ad-hoc ingestion for every compressed
batch of Zeek logs:

```bash
gunzip -c *.gz | vast import zeek
```

#### Broker

The `broker` import command ingests events via Zeek's
[Broker](https://github.com/zeek/broker) communication library.

Broker provides a topic-based publish-subscribe communication layer and
standardized data model to interact with the Zeek ecosystem. Using the `broker`
reader, VAST can transparently establish a connection to Zeek and subscribe log
events. Letting Zeek send events directly to VAST cuts out the operational
hassles of going through file-based logs.

To connect to a Zeek instance, run the `broker` command without arguments:

```bash
# Spawn a Broker endpoint, connect to localhost:9999/tcp, and subscribe
# to the topic `zeek/logs/` to acquire Zeek logs.
vast import broker
```

Logs should now flow from Zeek to VAST, assuming that Zeek has the following
default settings:

- The script variable `Broker::default_listen_address` is set to `127.0.0.1`.
  Zeek populates this variable with the value from the environment variable
  `ZEEK_DEFAULT_LISTEN_ADDRESS`, which defaults to `127.0.0.1`.
- The script variable `Broker::default_port` is set to `9999/tcp`.
- The script variable `Log::enable_remote_logging` is set to `T`.

Note: you can spawn Zeek with `Log::enable_local_logging=F` to avoid writing
additional local log files.

You can also spawn a Broker endpoint that is listening instead of connecting:

```bash
# Spawn a Broker endpoint, listen on localhost:8888/tcp, and subscribe
# to the topic `foo/bar`.
vast import broker --listen --port=8888 --topic=foo/bar
```

By default, VAST automatically subscribes to the topic `zeek/logs/` because
this is where Zeek publishes log events. Use `--topic` to set a different topic.

### Suricata

The `import suricata` command format consumes [EVE
JSON](https://suricata.readthedocs.io/en/latest/output/eve/eve-json-output.html)
logs from [Suricata](https://suricata-ids.org). Eve JSON is Suricata's unified
format to log all types of activity as single stream of [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON).

The Suricata "format" is a technically a JSON format, with a hard-coded
[selector](#map-events-to-schemas) that maps the value of the
`event_type` field to the prefix `suricata`.

Here's a Suricata `eve.log` example:

```json
{"timestamp":"2011-08-12T14:52:57.716360+0200","flow_id":1031464864740687,"pcap_cnt":83,"event_type":"alert","src_ip":"147.32.84.165","src_port":1181,"dest_ip":"78.40.125.4","dest_port":6667,"proto":"TCP","alert":{"action":"allowed","gid":1,"signature_id":2017318,"rev":4,"signature":"ET CURRENT_EVENTS SUSPICIOUS IRC - PRIVMSG *.(exe|tar|tgz|zip)  download command","category":"Potentially Bad Traffic","severity":2},"flow":{"pkts_toserver":27,"pkts_toclient":35,"bytes_toserver":2302,"bytes_toclient":4520,"start":"2011-08-12T14:47:24.357711+0200"},"payload":"UFJJVk1TRyAjemFyYXNhNDggOiBzbXNzLmV4ZSAoMzY4KQ0K","payload_printable":"PRIVMSG #zarasa48 : smss.exe (368)\r\n","stream":0,"packet":"AB5J2xnDCAAntbcZCABFAABMGV5AAIAGLlyTIFSlTih9BASdGgvw0QvAxUWHdVAY+rCL4gAAUFJJVk1TRyAjemFyYXNhNDggOiBzbXNzLmV4ZSAoMzY4KQ0K","packet_info":{"linktype":1}}
{"timestamp":"2011-08-12T14:55:22.154618+0200","flow_id":2247896271051770,"pcap_cnt":775,"event_type":"dns","src_ip":"147.32.84.165","src_port":1141,"dest_ip":"147.32.80.9","dest_port":53,"proto":"UDP","dns":{"type":"query","id":553,"rrname":"irc.freenode.net","rrtype":"A","tx_id":0}}
{"timestamp":"2011-08-12T16:59:22.181050+0200","flow_id":472067367468746,"pcap_cnt":25767,"event_type":"fileinfo","src_ip":"74.207.254.18","src_port":80,"dest_ip":"147.32.84.165","dest_port":1046,"proto":"TCP","http":{"hostname":"www.nmap.org","url":"/","http_user_agent":"Mozilla/4.0 (compatible)","http_content_type":"text/html","http_method":"GET","protocol":"HTTP/1.1","status":301,"redirect":"http://nmap.org/","length":301},"app_proto":"http","fileinfo":{"filename":"/","magic":"HTML document, ASCII text","gaps":false,"state":"CLOSED","md5":"70041821acf87389e40ddcb092004184","sha1":"10395ab3566395ca050232d2c1a0dbad69eb5fd2","sha256":"2e4c462b3424afcc04f43429d5f001e4ef9a28143bfeefb9af2254b4df3a7c1a","stored":true,"file_id":1,"size":301,"tx_id":0}}
```

Import the log as follows:

```bash
vast import suricata < eve.log
```

Instead of writing to a file, Suricata can also log to a UNIX domain socket that
VAST reads from. This requires the following settings in your `suricata.yaml`:

```yaml
outputs:
  - eve-log:
    enabled: yes
    filetype: unix_stream
    filename: eve.sock
```

To import from a UNIX domain socket, combine netcat with a `vast import`:

```bash
nc -vlkU eve.sock | vast import suricata
```

### NetFlow

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

### PCAP

VAST supports reading and writing [PCAP](http://www.tcpdump.org) traces via
`libpcap`. On the read path, VAST can either acquire packets from a trace file
or in *live mode* from a network interface.

While decapsulating packets, VAST extracts
[802.1Q](https://en.wikipedia.org/wiki/IEEE_802.1Q) VLAN tags into the nested
`vlan` record, consisting of an `outer` and `inner` field for the respective
tags. The value of the VLAN tag corresponds to the 12-bit VLAN identifier (VID).
Special values include `0` (frame does not carry a VLAN ID) and `0xFFF`
(reserved value; sometimes wildcard match).

In addition, VAST computes the [Community
ID](https://github.com/corelight/community-id-spec) per packet to support
pivoting from other log data. The packet record contains a field `community_id`
that represents the string representation of the Community ID, e.g.,
`1:wCb3OG7yAFWelaUydu0D+125CLM=`. If you prefer to not have the Community ID in
your data, add the option `--disable-community-id` to the `pcap` command.

To ingest a PCAP file `input.trace`, pass it to the `pcap` command on standard
input:

```bash
vast import pcap < input.trace
```

You can also acquire packets by listening on an interface:

```bash
vast import pcap -i eth0
```

#### Real-World Traffic Replay

When reading PCAP data from a trace, VAST processes packets directly one after
another. This differs from live packet capturing where there exists natural
inter-packet arrival times, according to the network traffic pattern. To emulate
"real-world" trace replay, VAST supports a *pseudo-realtime* mode, which works
by introducing inter-packet delays according to the difference between subsquent
packet timestamps.

The option `--pseudo-realtime`/`-p` takes a positive integer *c* to delay
packets by a factor of *1/c*. For example, if the first packet arrives at time
*t0* and the next packet at time *t1*, then VAST would sleep for time
*(t1 - t0)/c* before releasing the second packet. Intuitively, the larger *c*
gets, the faster the replay takes place.

For example, to replay packets as if they arrived in realtime, use `-p 1`. To
replay packets twice as fast as they arrived on the NIC, use `-p 2`.

#### Flow Management

The PCAP plugin has a few tuning knows for controlling storage of connection
data. Naive approaches, such as sampling or using a "snapshot" (`tcpdump -s`)
make transport-level analysis impractical due to an incomplete byte stream.
Inspired by the [Time Machine][tm], the PCAP plugin supports recording only the
first *N* bytes of a connection (the *cutoff*) and skipping the bulk of the flow
data. This allows for recording most connections in their entirety while
achieving a massive space reduction by forgoing the heavy tail of the traffic
distribution.

[tm]: http://www.icir.org/vern/papers/time-machine-sigcomm08.pdf

To record only the first 1,024 bytes every connection, pass `-c 1024` as option.
Not that the cut-off is *bi-directional*, i.e., it applies to both the
originator and responder TCP streams and a flow gets evicted only after both
sides have reached their cutoff value.

In addition to cutoff configuration, the PCAP plugin has a few other tuning
parameters. VAST keeps a flow table with per-connection state. The
`--max-flows`/`-m` option specifies an upper bound on the flow table size in
number of connections. After a certain amount of inactivity of a flow,
the corresponding state expires. The option `--max-flow-age`/`-a` controls this
timeout value. Finally, the frequency of when the flow table expires entries
can be controlled via `--flow-expiry`/`-e`.

### CEF

The [Common Event Format (CEF)][cef] is a text-based event format that
originally stems from ArcSight. It is line-based and human readable. The first 7
fields of a CEF event are always the same, and the 8th *extension* field is an
optional list of key-value pairs:

[cef]: https://community.microfocus.com/cfs-file/__key/communityserver-wikis-components-files/00-00-00-00-23/3731.CommonEventFormatV25.pdf

```
CEF:Version|Device Vendor|Device Product|Device Version|Device Event Class ID|Name|Severity|[Extension]
```

Here is a real-world example:

```
CEF:0|Cynet|Cynet 360|4.5.4.22139|0|Memory Pattern - Cobalt Strike Beacon ReflectiveLoader|8| externalId=6 clientId=2251997 scanGroupId=3 scanGroupName=Manually Installed Agents sev=High duser=tikasrv01\\administrator cat=END-POINT Alert dhost=TikaSrv01 src=172.31.5.93 filePath=c:\\windows\\temp\\javac.exe fname=javac.exe rt=3/30/2022 10:55:34 AM fileHash=2BD1650A7AC9A92FD227B2AB8782696F744DD177D94E8983A19491BF6C1389FD rtUtc=Mar 30 2022 10:55:34.688 dtUtc=Mar 30 2022 10:55:32.458 hostLS=2022-03-30 10:55:34 GMT+00:00 osVer=Windows Server 2016 Datacenter x64 1607 epsVer=4.5.5.6845 confVer=637842168250000000 prUser=tikasrv01\\administrator pParams="C:\\Windows\\Temp\\javac.exe" sign=Not signed pct=2022-03-30 10:55:27.140, 2022-03-30 10:52:40.222, 2022-03-30 10:52:39.609 pFileHash=1F955612E7DB9BB037751A89DAE78DFAF03D7C1BCC62DF2EF019F6CFE6D1BBA7 pprUser=tikasrv01\\administrator ppParams=C:\\Windows\\Explorer.EXE pssdeep=49152:2nxldYuopV6ZhcUYehydN7A0Fnvf2+ecNyO8w0w8A7/eFwIAD8j3:Gxj/7hUgsww8a0OD8j3 pSign=Signed and has certificate info gpFileHash=CFC6A18FC8FE7447ECD491345A32F0F10208F114B70A0E9D1CD72F6070D5B36F gpprUser=tikasrv01\\administrator gpParams=C:\\Windows\\system32\\userinit.exe gpssdeep=384:YtOYTIcNkWE9GHAoGLcVB5QGaRW5SmgydKz3fvnJYunOTBbsMoMH3nxENoWlymW:YLTVNkzGgoG+5BSmUfvJMdsq3xYu gpSign=Signed actRem=Kill, Rename
```

VAST's CEF plugin supports parsing such lines using the `cef` format:

```bash
vast import cef < cef.log
```

VAST translates the `extension` field to a nested record, where the key-value
pairs of the extensions map to record fields. Here is an example of the above
event:

```bash
vast export json 172.31.5.93 | jq
```

```json
{
  "cef_version": 0,
  "device_vendor": "Cynet",
  "device_product": "Cynet 360",
  "device_version": "4.5.4.22139",
  "signature_id": "0",
  "name": "Memory Pattern - Cobalt Strike Beacon ReflectiveLoader",
  "severity": "8",
  "extension": {
    "externalId": 6,
    "clientId": 2251997,
    "scanGroupId": 3,
    "scanGroupName": "Manually Installed Agents",
    "sev": "High",
    "duser": "tikasrv01\\administrator",
    "cat": "END-POINT Alert",
    "dhost": "TikaSrv01",
    "src": "172.31.5.93",
    "filePath": "c:\\windows\\temp\\javac.exe",
    "fname": "javac.exe",
    "rt": "3/30/2022 10:55:34 AM",
    "fileHash": "2BD1650A7AC9A92FD227B2AB8782696F744DD177D94E8983A19491BF6C1389FD",
    "rtUtc": "Mar 30 2022 10:55:34.688",
    "dtUtc": "Mar 30 2022 10:55:32.458",
    "hostLS": "2022-03-30 10:55:34 GMT+00:00",
    "osVer": "Windows Server 2016 Datacenter x64 1607",
    "epsVer": "4.5.5.6845",
    "confVer": 637842168250000000,
    "prUser": "tikasrv01\\administrator",
    "pParams": "C:\\Windows\\Temp\\javac.exe",
    "sign": "Not signed",
    "pct": "2022-03-30 10:55:27.140, 2022-03-30 10:52:40.222, 2022-03-30 10:52:39.609",
    "pFileHash": "1F955612E7DB9BB037751A89DAE78DFAF03D7C1BCC62DF2EF019F6CFE6D1BBA7",
    "pprUser": "tikasrv01\\administrator",
    "ppParams": "C:\\Windows\\Explorer.EXE",
    "pssdeep": "49152:2nxldYuopV6ZhcUYehydN7A0Fnvf2+ecNyO8w0w8A7/eFwIAD8j3:Gxj/7hUgsww8a0OD8j3",
    "pSign": "Signed and has certificate info",
    "gpFileHash": "CFC6A18FC8FE7447ECD491345A32F0F10208F114B70A0E9D1CD72F6070D5B36F",
    "gpprUser": "tikasrv01\\administrator",
    "gpParams": "C:\\Windows\\system32\\userinit.exe",
    "gpssdeep": "384:YtOYTIcNkWE9GHAoGLcVB5QGaRW5SmgydKz3fvnJYunOTBbsMoMH3nxENoWlymW:YLTVNkzGgoG+5BSmUfvJMdsq3xYu",
    "gpSign": "Signed",
    "actRem": "Kill, Rename"
  }
}
```

The [CEF specification][cef] pre-defines several extension field key names and
data types for the corresponding values. VAST's parser does not enforce the
strict definitions and instead tries to infer the type from the provided values.

### Argus

[Argus](https://qosient.com/argus/index.shtml) is an open-source flow monitor
that computes a variety of connection statistics. The UNIX tool `argus`
processes either PCAP or NetFlow data and generates binary output. The companion
utility `ra` transforms this binary output into a textual form that VAST can
parse.

Ingesting Argus data involves the following steps:

1. Read PCAP or NetFlow data with `argus`
2. Convert the binary Argus data into CSV with `ra`
3. Pipe the `ra` output to `vast`

#### Read network data

To read a PCAP file, simply pass a file via `-r`:

```bash
argus -r trace
```

To read from standard input, use `-r -`. Similarly, to write to standard
output, use `-w -`.

#### Convert Argus to CSV

Converting `argus` output to CSV requires the following flags:

- `-c ,` to enable CSV mode
- `-L0` to print a header with field names once
- `-n` suppress port nubmer to service conversions

The first column contains the timestamp, but unfortunately the default format
doesn't contain dates. Changing the timestamp format requires passing a
custom configuration file via `-F ra.conf` with the following contents:

```
RA_TIME_FORMAT="%y-%m-%d+%T.%f"
```

Finally, the `-s +a,b,c,...` flag includes list of field names that should be
appended after the default fields. Consult the manpage of `ra` under the `-s`
section for valid field names.

Put together, the following example generates valid CSV output for a PCAP file
called `trace.pcap`:

```
argus -r trace.pcap -w - |
  ra -F ra.conf -L0 -c , -n -s +spkts,dpkts,load,pcr
```

This generates the following output:

```
StartTime,Flgs,Proto,SrcAddr,Sport,Dir,DstAddr,Dport,TotPkts,TotBytes,State,SrcPkts,DstPkts,Load,PCRatio
09-11-18+09:00:03.914398, e        ,udp,192.168.1.1,626,   ->,224.0.0.1,626,1,75,INT,1,0,0.000000,-0.000000
09-11-18+09:00:20.093410, e        ,lldp,00:22:2d:81:db:10,0,   ->,01:80:c2:00:00:0e,0,1,118,INT,1,0,0.000000,-0.000000
09-11-18+09:00:21.486288, e        ,arp,192.168.1.102,,  who,192.168.1.1,,2,106,CON,1,1,0.000000,-0.000000
09-11-18+09:00:21.486539, e        ,udp,192.168.1.102,68,  <->,192.168.1.1,67,2,689,CON,1,1,0.000000,-0.000000
09-11-18+09:00:33.914396, e        ,udp,192.168.1.1,626,   ->,224.0.0.1,626,1,75,REQ,1,0,0.000000,-0.000000
09-11-18+09:00:50.208499, e        ,lldp,00:22:2d:81:db:10,0,   ->,01:80:c2:00:00:0e,0,1,118,REQ,1,0,0.000000,-0.000000
09-11-18+09:01:03.914408, e        ,udp,192.168.1.1,626,   ->,224.0.0.1,626,1,75,REQ,1,0,0.000000,-0.000000
09-11-18+09:01:20.323835, e        ,lldp,00:22:2d:81:db:10,0,   ->,01:80:c2:00:00:0e,0,1,118,REQ,1,0,0.000000,-0.000000
09-11-18+09:01:33.914414, e        ,udp,192.168.1.1,626,   ->,224.0.0.1,626,1,75,REQ,1,0,0.000000,-0.000000
```

#### Ingest Argus CSV

Since VAST has [native CSV support](#CSV), ingesting Argus CSV output only
requires an adequate schema. VAST already ships with an argus schema containing a
type `argus.record` that covers all fields from the `ra` man page.

The following command imports a file `argus.csv`:

```bash
vast import -t argus.record csv < argus.csv
```

Alternatively, this command pipeline processes a PCAP trace without
intermediate file and ships sends the data directory to VAST:

```
argus -r trace.pcap -w - |
  ra -F ra.conf -L0 -c , -n -s +spkts,dpkts,load,pcr |
  vast import -t argus.record csv
```

## Discard events from a data source

To reduce the volume of a data source or to filter out unwanted content, you can
provide a filter expression to the `import` command.

For example, you might want to import Suricata Eve JSON, but skip over all
events of type `suricata.stats`:

```bash
vast import suricata '#type != "suricata.stats"' < path/to/eve.json
```

See the [query language documentation](/docs/understand/query-language/) to
learn more about how to express filters.

## Infer a schema automatically

:::note Auto-inference underway
We have planned to make big improvements to the schema management. Most notably,
writing a schema will be optional in the future, i.e., only needed when tuning
data semantics.
:::

The `infer` command attempts to deduce a schema, given a sample of data. For
example, consider this JSON data:

import MissingDocumentation from '@site/presets/MissingDocumentation.md';

<MissingDocumentation/>

Run `head -1 data.json | vast infer` to print schema that you can paste into a
module.

<MissingDocumentation/>

The idea is that `infer` jump-starts the schema writing process by providing a
reasonable blueprint. You still need to provide the right name for the type and
perform adjustments, such as replacing some generic types with more semantic
aliases, e.g., using the `timstamp` alias instead of type `time` to designate
the event timestamp.

## Write a schema manually

If VAST does not ship with a [module][modules] for your data out of the box,
or the inference is not good enough for your use case regarding type semantics
or performance, you can easily write one yourself.

A schema is a record type with a name so that VAST can
represent it as a table internally. You would write a schema manually or extend
an existing schema if your goal is tuning type semantics and performance. For
example, if you have a field of type `string` that only holds IP addresses, you
can upgrade it to type `addr` and enjoy the benefits of richer query
expressions, e.g., top-k prefix search. Or if you onboard a new data source, you
can ship a schema along with [concept][concepts] mappings for a deeper
integration.

You write a schema (and potentially accompanying types, concepts, and models) in
a [module][modules].

Let's write one from scratch, for a tiny dummy data source called *foo* that
produces CSV events of this shape:

```csv
date,target,message
2022-05-17,10.0.0.1,foo
2022-05-18,10.0.0.2,bar
2022-05-18,10.0.0.3,bar
```

The corresponding schema type looks like this:

```yaml
message:
  record:
    - date: time
    - target: addr
    - message: msg
```

You can embed this type definition in a dedicated `foo` module:

```yaml
module: foo
types:
  message:
    record:
      - date: time
      - target: addr
      - message: msg
```

Now that you have a new module, you can choose to deploy it at the client or
the server. When a VAST server starts, it will send a copy of its local schemas
to the client. If the client has a schema for the same type, it will override
the server version. We recommend deploying the module at the server when all
clients should see the contained schemas, and at the client when the scope is
local. The diagram below illustrates the initial handshake:

![Schema Transfer](/img/schema-transfer.light.png#gh-light-mode-only)
![Schema Transfer](/img/schema-transfer.dark.png#gh-dark-mode-only)

Regardless of where you deploy the module, the procedure is the same at client
and server: place the module in an existing module directory, such as
`/etc/vast/modules`, or tell VAST in your `vast.yaml` configuration file where
to look for additional modules via the `module-dirs` key:

```yaml
vast:
  module-dirs:
    - path/to/modules
```

At the server, restart VAST and you're ready to go. Or just spin up a new client
and ingest the CSV with richer typing:

```bash
vast import csv < foo.csv
```

## Map events to schemas

For some input formats, such as JSON and CSV, VAST requires an existing schema
to find the corresponding type definition and use higher-level types.

There exist two ways to tell VAST how to map events to schemas:

1. **Field Matching**: by default, VAST checks every new record whether there
   exists a corresponding schema where the record fields match. If found, VAST
   automatically assigns the matching schema.

   The `--type=PREFIX` option makes it possible to restrict the set of candidate
   schemas to type names with a given prefix, in case there exist multiple
   schemas with identical field names. "Prefix" here means up to a dot delimiter
   or a full type name, e.g., `suricata` or `suricata.dns` are valid prefixes,
   but neither `suricat` nor `suricata.d`.

   :::info Performance Boost
   In case the prefix specified by `--type` yields *exactly one* possible
   candidate schema, VAST can operate substantially faster. The reason is that
   VAST disambiguates multiple schemas by comparing their normalized
   representation, which works by computing hash of the list of sorted field
   names and comparing it to the hash of the candidate types.
   :::

2. **Selector Specification**: some events have a dedicated field to indicate
   the type name of a particular event. For example, Suricata EVE JSON records
   have an `event_type` field that contains `flow`, `dns`, `smb`, etc., to
   signal what object structure to expect.

   To designate a selector field, use the `--selector=FIELD:PREFIX` option to
   specify a colon-separated field-name-to-schema-prefix mapping, e.g.,
   `vast import json --selector=event_type:suricata` reads the value from the
   field `event_type` and prefixes it with `suricata.` to look for a
   corresponding schema.

[types]: /docs/understand/data-model/type-system
[concepts]: /docs/understand/data-model/taxonomies#concepts
[modules]: /docs/understand/data-model/modules
