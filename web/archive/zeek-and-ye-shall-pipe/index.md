---
title: Zeek and Ye Shall Pipe
authors: mavam
date: 2023-07-13
tags: [zeek, logs, json, pipelines]
comments: true
---

# Zeek and Ye Shall Pipe

[Zeek](https://zeek.org) turns packets into structured logs. By default, Zeek
generates one file per log type and per rotation timeframe. If you don't want to
wrangle files and directly process the output, this short blog post is for you.

![Zeek as Pipeline](zeek-as-pipeline.excalidraw.svg)

<!-- truncate -->

Zeek requires a bit of adaptation to fit in the Unix pipeline model, by which we
mean *take your input on stdin and produce your output to stdout*:

```
<upstream> | zeek | <downstream>
```

In this example, `<upstream>` produces packets in PCAP format and `<downstream>`
processes the Zeek logs. Let's work towards this.

Solving the upstream part is easy: just use `zeek -r -` to read from stdin. So
let's focus on the logs downstream. [Our last blog](/blog/mobilizing-zeek-logs)
introduced the various logging formats, such as tab-separated values (TSV),
JSON, and Streaming JSON with an extra `_path` discriminator field. The only
format conducive to multiplexing different log types is Streaming JSON.

Let's see what we get:

```bash
zcat < trace.pcap | zeek -r - json-streaming-logs
```

```
❯ ls
json_streaming_analyzer.1.log       json_streaming_packet_filter.1.log
json_streaming_conn.1.log           json_streaming_pe.1.log
json_streaming_dce_rpc.1.log        json_streaming_reporter.1.log
json_streaming_dhcp.1.log           json_streaming_sip.1.log
json_streaming_dns.1.log            json_streaming_smb_files.1.log
json_streaming_dpd.1.log            json_streaming_smb_mapping.1.log
json_streaming_files.1.log          json_streaming_snmp.1.log
json_streaming_http.1.log           json_streaming_ssl.1.log
json_streaming_kerberos.1.log       json_streaming_tunnel.1.log
json_streaming_ntlm.1.log           json_streaming_weird.1.log
json_streaming_ntp.1.log            json_streaming_x509.1.log
json_streaming_ocsp.1.log
```

The `json-streaming-package` prepends a distinguishing prefix to the filename.
The `*.N.log` suffix counts the rotations, e.g., `*.1.log` means the logs from
the first batch.

Let's try to avoid the files altogether and send the contents of these file to
stdout. This requires a bit of option fiddling to achieve the desired result:

```bash
zcat < trace.pcap |
  zeek -r - \
    LogAscii::output_to_stdout=T \
    JSONStreaming::disable_default_logs=T \
    JSONStreaming::enable_log_rotation=F \
    json-streaming-logs
```

This requires a bit explanation:

- `LogAscii::output_to_stdout=T` redirects the log output to stdout.
- `JSONStreaming::disable_default_logs=T` disables the default TSV logs.
  Without this option, Zeek will print *both* TSV and NDJSON to stdout.
- `JSONStreaming::enable_log_rotation=F` disables log rotation. This is needed
  because the option `output_to_stdout=T` sets the internal filenames to
  `/dev/stdout`, which Zeek then tries to rotate away. Better not.

Here's the result you'd expect, which is basically a `cat *.log`:

```json
{"_path":"files","_write_ts":"2021-11-17T13:32:43.250616Z","ts":"2021-11-17T13:32:43.250616Z","fuid":"FhEFqzHx1hVpkhWci","uid":"CHhfpE1dTbPgBTR24","id.orig_h":"128.14.134.170","id.orig_p":57468,"id.resp_h":"198.71.247.91","id.resp_p":80,"source":"HTTP","depth":0,"analyzers":[],"mime_type":"text/html","duration":0.0,"is_orig":false,"seen_bytes":51,"total_bytes":51,"missing_bytes":0,"overflow_bytes":0,"timedout":false}
{"_path":"http","_write_ts":"2021-11-17T13:32:43.250616Z","ts":"2021-11-17T13:32:43.249475Z","uid":"CHhfpE1dTbPgBTR24","id.orig_h":"128.14.134.170","id.orig_p":57468,"id.resp_h":"198.71.247.91","id.resp_p":80,"trans_depth":1,"method":"GET","host":"198.71.247.91","uri":"/","version":"1.1","user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36 ","request_body_len":0,"response_body_len":51,"status_code":200,"status_msg":"OK","tags":[],"resp_fuids":["FhEFqzHx1hVpkhWci"],"resp_mime_types":["text/html"]}
{"_path":"packet_filter","_write_ts":"1970-01-01T00:00:00.000000Z","ts":"2023-07-11T03:30:17.189787Z","node":"zeek","filter":"ip or not ip","init":true,"success":true}
{"_path":"conn","_write_ts":"2021-11-17T13:33:01.457108Z","ts":"2021-11-17T13:32:46.565338Z","uid":"CD868huwhDP636oT","id.orig_h":"89.248.165.145","id.orig_p":43831,"id.resp_h":"198.71.247.91","id.resp_p":52806,"proto":"tcp","conn_state":"S0","missed_bytes":0,"history":"S","orig_pkts":1,"orig_ip_bytes":40,"resp_pkts":0,"resp_ip_bytes":0}
{"_path":"tunnel","_write_ts":"2021-11-17T13:40:34.891453Z","ts":"2021-11-17T13:40:34.891453Z","uid":"CsqzCG2F8VDR4gM3a8","id.orig_h":"49.213.162.198","id.orig_p":0,"id.resp_h":"198.71.247.91","id.resp_p":0,"tunnel_type":"Tunnel::GRE","action":"Tunnel::DISCOVER"}
```

Nobody can remember this invocation. Especially during firefighting when you
quickly need to plow through a trace to understand it. So we want to wrap this
somehow:

```bash title=zeekify
#!/bin/sh
zeek -r - \
  LogAscii::output_to_stdout=T \
  JSONStreaming::disable_default_logs=T \
  JSONStreaming::enable_log_rotation=F \
  json-streaming-logs \
  "$@"
```

Now we're in pipeline land:

```bash
zcat pcap.gz | zeekify | head | jq -r ._path
```

```
packet_filter
files
ntp
tunnel
conn
ntp
http
conn
ntp
conn
```

Okay, we got Zeek as a Unix pipe. But now you have to wrangle the JSON with
`jq`. Unless you're a die-hard fan, even simple analytics, like filtering or
aggregating, have a steep learning curve. In the next blog post, we'll double
down on the elegant principle of pipelines and show how you can take do easy
in-situ analytics with Tenzir.
