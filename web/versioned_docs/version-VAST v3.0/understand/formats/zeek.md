---
description: Open source network security monitor
---

# Zeek

The [Zeek](https://zeek.org) network security monitor comes with its own format
for representing logs. Zeek's tab-separated value (TSV) representation includes
additional header fields with field names, type annotations, and additional
metadata.

Zeek can also render its logs as JSON. There are two ways to do that:

1. One log type per file, similar to TSV, but where each file consists of
   homogeneous NDJSON records. To enable this log style, add
   [`LogAscii::use_json=T`](https://docs.zeek.org/en/master/frameworks/logging.html)
   to your Zeek command line invocation or script configuration.

2. A single file stream of heterogeneous records when using the
   [json-streaming-logs](https://github.com/corelight/json-streaming-logs)
   package. This mode adds a `_path` field to disambiguate the log type.

## Parser

The `import zeek` command consumes TSV logs, whereas `import zeek-json` consumes
JSON logs.

1. For stock Zeek JSON logs with one log type per file, use the existing `import
   json` with the `--type` option to specify the Zeek log type.
2. For JSON streaming logs, use the `import zeek-json` command, which will
   automatically look at the value of the `_path` to demultiplex the log stream.

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

:::info type mapping
The `import zeek` command maps the types `count`, `real`, and `addr` from the
Zeek TSV `#types` header line to VAST's basic types `uint64`, `double`, and
`ip`.
:::

When Zeek [rotates logs][zeek-log-rotation], it produces compressed batches of
`*.log.gz` regularly. If log freshness is not a priority, you could trigger an
ad-hoc ingestion for every compressed batch of Zeek logs:

```bash
gunzip -c *.gz | vast import zeek
```

[zeek-log-rotation]: https://docs.zeek.org/en/stable/frameworks/logging.html#rotation

## Printer

Zeek's TSV model represents effectively a transposed record, with nested record
fields being dot-separated. This makes it feasible to re-export a subset of data
from VAST as Zeek logs. Only a subset because there exist some restrictions,
such as records within lists.

For example, assume this Suricata EVE JSON log is in VAST:

```json
{"timestamp":"2011-08-14T07:38:53.914038+0200","flow_id":929669869939483,"event_type":"flow","src_ip":"147.32.84.165","src_port":138,"dest_ip":"147.32.84.255","dest_port":138,"proto":"UDP","app_proto":"failed","flow":{"pkts_toserver":2,"pkts_toclient":0,"bytes_toserver":486,"bytes_toclient":0,"start":"2011-08-12T14:53:47.928539+0200","end":"2011-08-12T14:53:47.928552+0200","age":0,"state":"new","reason":"timeout","alerted":false}}
{"timestamp":"2011-08-12T15:00:36.378914+0200","flow_id":269421754201300,"pcap_cnt":22569,"event_type":"http","src_ip":"147.32.84.165","src_port":1027,"dest_ip":"74.125.232.202","dest_port":80,"proto":"TCP","tx_id":0,"http":{"hostname":"cr-tools.clients.google.com","url":"/service/check2?appid=%7B430FD4D0-B729-4F61-AA34-91526481799D%7D&appversion=1.3.21.65&applang=&machine=0&version=1.3.21.65&osversion=5.1&servicepack=Service%20Pack%202","http_user_agent":"Google Update/1.3.21.65;winhttp","http_method":"GET","protocol":"HTTP/1.1","length":0}}
```

You can export it as Zeek log as follows:

```bash
vast export zeek '#type == /.*(flow|http)/'
```

```
#separator 	
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	suricata.flow
#open	2022-12-16-20-24-23
#fields	timestamp	flow_id	pcap_cnt	vlan	in_iface	src_ip	src_port	dest_ip	dest_port	proto	event_type	community_id	flow.pkts_toserver	flow.pkts_toclient	flow.bytes_toserver	flow.bytes_toclient	flow.start	flow.end	flow.age	flow.state	flow.reason	flow.alerted	app_proto
#types	time	count	count	vector[count]	string	addr	port	addr	port	string	string	string	count	count	count	countime	time	count	string	string	bool	string
1313300333.914038	929669869939483	-	-	-	147.32.84.165	138	147.32.84.255	138	UDP	flow	-	2	486	0	1313153627.928539	1313153627.928552	0	new	timeout	F	failed
#separator 	
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	suricata.http
#open	2022-12-16-20-24-23
#fields	timestamp	flow_id	pcap_cnt	vlan	in_iface	src_ip	src_port	dest_ip	dest_port	proto	event_type	community_id	http.hostname	http.url	http.http_port	http.http_user_agent	http.http_content_type	http.http_method	http.http_refer	http.protocol	http.status	http.redirect	http.length	tx_id
#types	time	count	count	vector[count]	string	addr	port	addr	port	string	string	string	string	string	port	string	string	string	string	string	count	string	count	count
1313154036.378914	269421754201300	22569	-	-	147.32.84.165	1027	74.125.232.202	80	TCP	http	-	cr-tools.clients.google.com	/service/check2?appid=%7B430FD4D0-B729-4F61-AA34-91526481799D%7D&appversion=1.3.21.65&applang=&machine=0&version=1.3.21.65&osversion=5.1&servicepack=Service%20Pack%202	-	Google Update/1.3.21.65;winhttp	-	GET	-	HTTP/1.1	-	0
#close	2022-12-16-20-24-23
```

The Zeek TSV format cannot display data from multiple schemas. VAST prints a
header for following lines whenever it encounters a schema change.
