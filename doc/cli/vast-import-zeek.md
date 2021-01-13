The `import zeek` command consumes [Zeek](https://zeek.org) logs in
tab-separated value (TSV) style, and the `import zeek-json` command consumes
Zeek logs as [line-delimited
JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON) objects
as produced by the
[json-streaming-logs](https://github.com/corelight/json-streaming-logs) package.
Unlike stock Zeek JSON logs, where one file contains exactly one log type, the
streaming format contains different log event types in a single stream and uses
an additional `_path` field to disambiguate the log type. For stock Zeek JSON
logs, use the existing `import json` with the `-t` flag to specify the log type.

Here's an example of a typical Zeek `conn.log`:

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

When Zeek [rotates
logs](https://docs.zeek.org/en/stable/frameworks/logging.html#rotation), it
produces compressed batches of `*.tar.gz` regularly. Ingesting a compressed
batch involves unpacking and concatenating the input before sending it to VAST:

```
gunzip -c *.gz | vast import zeek
```
