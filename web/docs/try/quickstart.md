---
sidebar_position: 1
---

# Quickstart

This guide illustrates how you can use VAST from the command line interface.
We're assuming that you have [installed VAST](../setup/install/README.md) and
that you have a `vast` binary in your path.

## Start a VAST node

Let's spin up a VAST node:

```bash
vast start
```

Let's connect to the node and check its version via the `status` command:

```bash
vast status version
```

```json
{
  "version": {
    "Apache Arrow": "10.0.1",
    "Build Configuration": {
      "Address Sanitizer": false,
      "Assertions": false,
      "Tree Hash": "30f123e44a2387d473fd22e90b893514",
      "Type": "Release",
      "Undefined Behavior Sanitizer": false
    },
    "CAF": "0.18.7",
    "VAST": "v3.0.0-rc1-2-gcc393580f1",
    "plugins": []
  }
}
```

## Download example dataset

Now that we have a VAST node running, let’s ingest some data. We [prepared a
dataset][m57] derived from one day of the M57 recording and injected malicious
traffic from malware-traffic-analysis.net, adjusting timestamps such that the
malware activity occurs in the same day as the background noise. Then we ran
[Zeek](https://zeek.org) and [Suricata](https://suricata.io) over the PCAP to
get structured log data.

[m57]: https://storage.googleapis.com/vast-datasets/M57/README.md

First download the logs:

``` bash
cd /tmp
curl -L -O https://storage.googleapis.com/vast-datasets/M57/suricata.tar.gz
curl -L -O https://storage.googleapis.com/vast-datasets/M57/zeek.tar.gz
```

Then ingest them via `vast import`, which spawns a dedicated process that
handles the parsing, followed by sending batches of data to our VAST node:

```bash
# The 'O' flag in tar dumps the archive contents to stdout.
tar xOzf zeek.tar.gz | vast import zeek
```

```
[20:35:58.434] client connected to VAST node at 127.0.0.1:5158
[20:36:00.923] zeek-reader source produced 954189 events at a rate of 383606 events/sec in 2.49s
```

```bash
tar xOzf suricata.tar.gz | vast import suricata '#type == "suricata.alert"'
```

```
[20:36:42.380] client connected to VAST node at 127.0.0.1:5158
[20:36:52.420] suricata-reader source produced 1398847 events at a rate of 139397 events/sec in 10.03s
[20:36:58.075] suricata-reader source produced 750510 events at a rate of 132718 events/sec in 5.65s
```

We've added the import filter
[expression](../understand/expressions.md) `#type == "suricata.alert"`
because we're want the alerts from Suricata and the metadata from Zeek.

:::note Multi-schema Zeek TSV Parser
While the Suricata tarball contains just a single `eve.json` file, the Zeek
tarball includes numerous logs:

<details>
<summary>Zeek tarball contents</summary>

```bash
tar tf zeek.tar.gz
```

```
Zeek/
Zeek/ntlm.log
Zeek/loaded_scripts.log
Zeek/ftp.log
Zeek/dhcp.log
Zeek/notice.log
Zeek/smtp.log
Zeek/reporter.log
Zeek/x509.log
Zeek/pe.log
Zeek/conn.log
Zeek/snmp.log
Zeek/smb_files.log
Zeek/stats.log
Zeek/capture_loss.log
Zeek/ssl.log
Zeek/sip.log
Zeek/smb_mapping.log
Zeek/files.log
Zeek/analyzer.log
Zeek/radius.log
Zeek/tunnel.log
Zeek/dpd.log
Zeek/http.log
Zeek/ntp.log
Zeek/telemetry.log
Zeek/packet_filter.log
Zeek/traceroute.log
Zeek/weird.log
Zeek/dns.log
Zeek/kerberos.log
Zeek/ocsp.log
Zeek/dce_rpc.log
```

</details>

VAST's Zeek TSV parser auto-detects schema changes in the same stream of input
and resets itself whenever it encounters a new log header. This is why piping a
bunch of logs to `vast import zeek` Just Works.
:::

## Export data

With Zeek and Suricata data in the VAST node, let's run some query pipelines.
We're going to show the data in [JSON format](../understand/formats/json.md),
but you could display it in [other formats](../understand/formats/README.md) as
well.

:::info Pipelines
As of VAST v3.0, the VAST language supports ad-hoc
[pipelines](../understand/pipelines.md) for flexible transformation in addition
to plain search. A pipeline consists of a chain of
[operators](../understand/operators/README.md) and must begin with *source*
operator, the producer emitting data, and end with a *sink* operator, the
consumer receiving data. If a pipeline has source and sink, we call it *closed*.
The `export` command implicitly closes a pipeline by adding the VAST node as
source and standard output as sink.

Concretely, `export` takes a pipeline of the form `EXPR | OP | OP` where `EXPR`
is an [expression](../understand/expressions.md) followed by zero or
more operators. The full pipeline would be:

```
from vast
| where EXPR
| OP
| OP
| to stdout
```

In future versions of VAST, you will gain full control over sources and sinks.
Stay tuned.
:::

### Understand the shape of the data

If you know what you want, you can dive right into querying data with
expressions. But you can also take a step back and inspect the schema metadata
VAST keeps using the `show` command.

```bash
vast show schemas --yaml
```

```yaml
# Excerpt only
- zeek.conn:
    record:
      - ts:
          timestamp: time
      - uid:
          type: string
          attributes:
            index: hash
      - id:
          zeek.conn_id:
            record:
              - orig_h: ip
              - orig_p:
                  port: uint64
              - resp_h: ip
              - resp_p:
                  port: uint64
      - proto: string
      - service: string
      - duration: duration
      - orig_bytes: uint64
      - resp_bytes: uint64
      - conn_state: string
      - ...
```

This example output showcases the schema for a Zeek conn.log. You can see the
various fields as list of key-value pairs under the `record` key. Note the
nested record `id` that has type alias called `zeek.conn_id`.

:::info Schemas
In VAST, a [schema](../understand/data-model/schemas.md) is just a record [type
definition](../understand/data-model/type-system.md). You would touch schemas
when the defaults are not enough. In future versions of VAST, you will interact
less and less with schemas because VAST can actually infer them reasonably well.
:::

Now that you know a little bit about available schemas of the data, you could
start referencing record fields in expressions. But VAST can also give you a
taste of actual events. The
[`taste`](../understand/operators/transformations/taste.md) operator limits the
number of events per unique schema:

```bash
vast export json '#type == /(zeek|suricata).*/ | taste 1'
```

```json
{"ts":"2021-11-18T08:04:34.309665","id":null,"certificate":{"version":3,"serial":"0FD9082ED1DED61FFBDAD4088EEDD543","subject":"CN=sni.cloudflaressl.com,O=Cloudflare\\, Inc.,L=San Francisco,ST=California,C=US","issuer":"CN=Cloudflare Inc ECC CA-3,O=Cloudflare\\, Inc.,C=US","not_valid_before":"2021-06-10T22:00:00.000000","not_valid_after":"2022-06-10T21:59:59.000000","key_alg":"id-ecPublicKey","sig_alg":"ecdsa-with-SHA256","key_type":"ecdsa","key_length":256,"exponent":null,"curve":"prime256v1"},"san":{"dns":["is.gd","*.is.gd","sni.cloudflaressl.com"],"uri":null,"email":null,"ip":null},"basic_constraints":{"ca":false,"path_len":null},"_write_ts":null}
{"ts":"2021-11-18T16:56:25.983171","src":"10.12.7.64","dst":"69.28.162.0","proto":"icmp"}
{"ts":"2023-03-07T09:23:46.814130","node":"zeek","filter":"ip or not ip","init":true,"success":true,"_write_ts":null}
{"ts":"2021-11-18T08:30:39.924381","uid":"CFGmQJ2SFUo712tGl7","id":{"orig_h":"10.12.14.101","orig_p":62434,"resp_h":"10.12.14.14","resp_p":445},"username":"maggie.simpson","hostname":"DESKTOP-RJ28EGJ","domainname":"PETAL-STARS","server_nb_computer_name":"PETAL-STARS-DC","server_dns_computer_name":"Petal-Stars-DC.petal-stars.com","server_tree_name":"petal-stars.com","success":true,"status":null,"_write_ts":null}
{"ts":"2021-11-17T13:32:43.237881","peer":"zeek","mem":77,"pkts_proc":1,"bytes_recv":74,"pkts_dropped":null,"pkts_link":null,"pkt_lag":null,"events_proc":525,"events_queued":18,"active_tcp_conns":1,"active_udp_conns":0,"active_icmp_conns":0,"tcp_conns":1,"udp_conns":0,"icmp_conns":0,"timers":55,"active_timers":47,"files":0,"active_files":0,"dns_requests":0,"active_dns_requests":0,"reassem_tcp_size":0,"reassem_file_size":0,"reassem_frag_size":0,"reassem_unknown_size":0,"_write_ts":null}
{"ts":"2021-11-17T20:29:19.245835","level":"Reporter::ERROR","message":"connection does not have analyzer specified to disable","location":"","_write_ts":null}
{"ts":"2021-11-18T08:04:34.309699","id":"FdVCda4K8lERFgmrQk","hashAlgorithm":"sha1","issuerNameHash":"12D78B402C356206FA827F8ED8922411B4ACF504","issuerKeyHash":"A5CE37EAEBB0750E946788B445FAD9241087961F","serialNumber":"0FD9082ED1DED61FFBDAD4088EEDD543","certStatus":"good","revoketime":null,"revokereason":null,"thisUpdate":"2022-02-12T05:27:01.000000","nextUpdate":"2022-02-19T04:42:01.000000"}
{"ts":"2021-11-17T13:49:03.961462","uid":null,"id":{"orig_h":null,"orig_p":null,"resp_h":null,"resp_p":null},"fuid":null,"file_mime_type":null,"file_desc":null,"proto":null,"note":"CaptureLoss::Too_Little_Traffic","msg":"Only observed 0 TCP ACKs and was expecting at least 1.","sub":null,"src":null,"dst":null,"p":null,"n":null,"peer_descr":null,"actions":["Notice::ACTION_LOG"],"suppress_for":"1.0h","dropped":null,"remote_location":{"country_code":null,"region":null,"city":null,"latitude":null,"longitude":null},"_write_ts":null}
{"ts":"2021-11-18T09:56:39.662996","uid":"CBTne9tomX1ktuCQa","id":{"orig_h":"10.4.21.101","orig_p":53824,"resp_h":"107.23.103.216","resp_p":587},"trans_depth":1,"helo":"localhost","mailfrom":"westland@brightsmiledental.biz","rcptto":["mariamchombo@info.com"],"date":"Tue, 21 Apr 2020 12:34:37 +0000","from":"\"http://www.optumengineering.com/contact.php\" <westland@brightsmiledental.biz>","to":["mariamchombo@info.com"],"cc":null,"reply_to":null,"msg_id":"<6a0ed21d-9b91-4aa1-8348-5846ecaa0d1d@local>","in_reply_to":null,"subject":"RE: Can we Invest","x_originating_ip":null,"first_received":null,"second_received":null,"last_reply":"250 OK id=1jQs7O-0007uj-OR","path":["107.23.103.216","10.4.21.101"],"user_agent":null,"tls":false,"fuids":["FD6v174ApeTME1v985","FP6rou2DmOcgl5sKtg"],"is_webmail":false,"_write_ts":null}
{"name":"/opt/homebrew/Cellar/zeek/5.2.0/share/zeek/base/init-bare.zeek","_write_ts":null}
{"ts":"2021-11-17T14:02:38.165570","uid":"CnrwFesjfOhI3fuu1","id":{"orig_h":"45.137.23.27","orig_p":47958,"resp_h":"198.71.247.91","resp_p":53},"proto":"udp","trans_id":13551,"rtt":null,"query":"version.bind","qclass":3,"qclass_name":"C_CHAOS","qtype":16,"qtype_name":"TXT","rcode":null,"rcode_name":null,"AA":false,"TC":false,"RD":true,"RA":false,"Z":0,"answers":null,"TTLs":null,"rejected":false,"_write_ts":null}
{"ts":"2021-11-18T08:18:58.213302","uid":"CWb7Iv2BSLQS2X2xsd","id.orig_h":"10.2.9.133","id.orig_p":49799,"id.resp_h":"10.2.9.9","id.resp_p":445,"id.vlan":null,"id.vlan_inner":null,"path":"\\\\VINYARDSONGS-DC.vinyardsongs.com\\IPC$","service":null,"native_file_system":null,"share_type":"PIPE"}
{"ts":"2021-11-17T13:32:46.565337","uid":"C5luJD1ATrGDOcouW2","id":{"orig_h":"89.248.165.145","orig_p":43831,"resp_h":"198.71.247.91","resp_p":52806},"proto":"tcp","service":null,"duration":null,"orig_bytes":null,"resp_bytes":null,"conn_state":"S0","local_orig":null,"local_resp":null,"missed_bytes":0,"history":"S","orig_pkts":1,"orig_ip_bytes":40,"resp_pkts":0,"resp_ip_bytes":0,"tunnel_parents":null,"community_id":"1:c/CLmyk4xRElyzleEMhJ4Baf4Gk=","_write_ts":null}
{"ts":"2021-11-18T08:09:55.857809","id":"FYQWgyssG9CNDD9m5","machine":"AMD64","compile_ts":"2021-05-15T03:54:23.000000","os":"Windows Vista or Server 2008","subsystem":"WINDOWS_CUI","is_exe":true,"is_64bit":true,"uses_aslr":true,"uses_dep":true,"uses_code_integrity":false,"uses_seh":true,"has_import_table":false,"has_export_table":false,"has_cert_table":true,"has_debug_data":true,"section_names":[".rdata",".rsrc"],"_write_ts":null}
{"ts":"2021-11-17T13:32:43.237881","peer":"zeek","metric_type":"gauge","prefix":"process","name":"resident_memory","unit":"bytes","labels":[],"label_values":[],"value":0}
{"ts":"2021-11-17T18:30:34.729549","uid":"CbgsR920H3wcrQYxJ2","id":{"orig_h":"147.203.255.20","orig_p":35370,"resp_h":"198.71.247.91","resp_p":161},"duration":"0.0ns","version":"3","community":null,"get_requests":0,"get_bulk_requests":0,"get_responses":0,"set_requests":0,"display_string":null,"up_since":null,"_write_ts":null}
{"ts":"2021-11-17T20:29:19.244890","uid":"C9X18448hrc02CUtp1","id":{"orig_h":"64.227.184.169","orig_p":36306,"resp_h":"198.71.247.91","resp_p":80},"version":null,"cipher":null,"curve":null,"server_name":"198.71.247.91","resumed":false,"last_alert":null,"next_protocol":null,"established":false,"cert_chain_fuids":null,"client_cert_chain_fuids":null,"subject":null,"issuer":null,"client_subject":null,"client_issuer":null,"validation_status":null,"ja3":"49826454e14fe89f32bec2f79ddadf73","_write_ts":null}
{"ts":"2021-11-17T14:22:27.640723","uid":"CjErQv2Au67bQPQ3E1","id":{"orig_h":"178.239.21.147","orig_p":5329,"resp_h":"198.71.247.91","resp_p":5060},"trans_depth":0,"method":"OPTIONS","uri":"sip:100@198.71.247.91","date":null,"request_from":"\"sipvicious\"<sip:100@1.1.1.1>","request_to":"\"sipvicious\"<sip:100@1.1.1.1>","response_from":null,"response_to":null,"reply_to":null,"call_id":"677136566775631087127887","seq":"1 OPTIONS","subject":null,"request_path":["SIP/2.0/UDP 178.239.21.147:5329"],"response_path":[],"user_agent":"friendly-scanner","status_code":null,"status_msg":null,"warning":null,"request_body_len":0,"response_body_len":null,"content_type":null,"_write_ts":null}
{"ts":"2021-11-17T13:32:43.250616","fuid":"FhEFqzHx1hVpkhWci","tx_hosts":null,"rx_hosts":null,"conn_uids":null,"source":"HTTP","depth":0,"analyzers":["MD5","SHA1"],"mime_type":"text/html","filename":null,"duration":"0.0ns","local_orig":null,"is_orig":false,"seen_bytes":51,"total_bytes":51,"missing_bytes":0,"overflow_bytes":0,"timedout":false,"parent_fuid":null,"md5":"112dedb4a4ef2d760fc6d8428e303789","sha1":"18b182056b32f6cdb1e7ef2abcad7c7ab62e609e","sha256":null,"extracted":null,"extracted_cutoff":null,"extracted_size":null,"_write_ts":null}
{"ts":"2021-11-17T13:33:53.748229","ts_delta":"1.18m","peer":"zeek","gaps":0,"acks":2,"percent_lost":0,"_write_ts":null}
{"ts":"2021-11-18T08:30:46.953822","uid":"C72eDz2CrVVb0lI66","id.orig_h":"10.12.14.101","id.orig_p":62439,"id.resp_h":"10.12.14.14","id.resp_p":445,"id.vlan":null,"id.vlan_inner":null,"fuid":null,"action":"SMB::FILE_OPEN","path":"\\\\Petal-Stars-DC\\shared","name":"<share_root>","size":0,"prev_name":null,"times.modified":"2020-12-10T15:30:59.163533","times.accessed":"2020-12-10T15:30:59.163533","times.created":"2020-12-10T15:30:59.163533","times.changed":"2020-12-10T18:51:17.176378","data_offset_req":null,"data_len_req":null,"data_len_rsp":null}
{"ts":"2021-11-17T16:08:51.164973","uid":"CWRjLsw4qcfKQfbF2","id":{"orig_h":"36.33.46.185","orig_p":52238,"resp_h":"198.71.247.91","resp_p":80},"proto":"tcp","analyzer":"HTTP","failure_reason":"not a http request line","_write_ts":null}
{"ts":"2021-11-17T13:38:55.448187","uid":"CjgqiM3DfHEf6odyxk","id.orig_h":"205.185.126.251","id.orig_p":45245,"id.resp_h":"198.71.247.91","id.resp_p":123,"id.vlan":null,"id.vlan_inner":null,"version":4,"mode":3,"stratum":0,"poll":"16.0s","precision":"15.62ms","root_delay":"1.0s","root_disp":"1.0s","ref_id":"\u0000\u0000\u0000\u0000","ref_time":"1970-01-01T00:00:00.000000","org_time":"1970-01-01T00:00:00.000000","rec_time":"1970-01-01T00:00:00.000000","xmt_time":"2004-11-24T15:12:11.444112","num_exts":0}
{"ts":"2021-11-17T13:32:43.249475","uid":"CZwqhx3td8eTfCSwJb","id":{"orig_h":"128.14.134.170","orig_p":57468,"resp_h":"198.71.247.91","resp_p":80},"trans_depth":1,"method":"GET","host":"198.71.247.91","uri":"/","referrer":null,"version":"1.1","user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36 ","request_body_len":0,"response_body_len":51,"status_code":200,"status_msg":"OK","info_code":null,"info_msg":null,"tags":[],"username":null,"password":null,"proxied":null,"orig_fuids":null,"orig_filenames":null,"orig_mime_types":null,"resp_fuids":["FhEFqzHx1hVpkhWci"],"resp_filenames":null,"resp_mime_types":["text/html"],"_write_ts":null}
{"timestamp":"2021-11-17T13:49:34.301057","flow_id":2006383376373761,"pcap_cnt":123,"vlan":null,"in_iface":null,"src_ip":"8.218.64.104","src_port":8000,"dest_ip":"198.71.247.91","dest_port":1027,"proto":"UDP","event_type":"alert","community_id":null,"alert":{"app_proto":null,"action":"allowed","gid":1,"signature_id":2200075,"rev":2,"signature":"SURICATA UDPv4 invalid checksum","category":"Generic Protocol Command Decode","severity":3,"source":{"ip":null,"port":null},"target":{"ip":null,"port":null}},"flow":{"pkts_toserver":1,"pkts_toclient":0,"bytes_toserver":49,"bytes_toclient":0,"start":"2021-11-17T13:49:34.301057","end":null,"age":null,"state":null,"reason":null,"alerted":null},"payload":null,"payload_printable":null,"stream":null,"packet":null,"packet_info":{"linktype":null}}
{"ts":"2021-11-17T13:40:34.891452","uid":"CWjQ8P1FF0z7r5AbDk","id":{"orig_h":"49.213.162.198","orig_p":0,"resp_h":"198.71.247.91","resp_p":0},"tunnel_type":"Tunnel::GRE","action":"Tunnel::DISCOVER","_write_ts":null}
{"ts":"2021-11-17T13:54:01.721755","uid":"Cqp7rtziLijlnrxYf","id":{"orig_h":"87.251.64.137","orig_p":64078,"resp_h":"198.71.247.91","resp_p":80},"name":"bad_HTTP_request","addl":null,"notice":false,"peer":"zeek","_write_ts":null}
{"ts":"2021-11-19T01:28:01.911438","uid":"CU691X1aVtChEW3AGj","id":{"orig_h":"192.168.1.105","orig_p":49306,"resp_h":"143.166.11.10","resp_p":21},"user":"anonymous","password":"IEUser@","command":"PASV","arg":null,"mime_type":null,"file_size":null,"reply_code":227,"reply_msg":"Entering Passive Mode (143,166,11,10,250,53)","data_channel":{"passive":true,"orig_h":"192.168.1.105","resp_h":"143.166.11.10","resp_p":64053},"fuid":null,"_write_ts":null}
{"ts":"2021-11-17T13:54:01.721755","cause":"violation","analyzer_kind":"protocol","analyzer_name":"HTTP","uid":"Cqp7rtziLijlnrxYf","fuid":null,"id.orig_h":"87.251.64.137","id.orig_p":64078,"id.resp_h":"198.71.247.91","id.resp_p":80,"id.vlan":null,"id.vlan_inner":null,"failure_reason":"not a http request line","failure_data":null}
{"ts":"2021-11-22T09:55:17.023885","uid":"CcVKAW2r2x6EiSqW47","id":{"orig_h":"183.136.225.42","orig_p":27827,"resp_h":"198.71.247.91","resp_p":1812},"username":null,"result":"unknown","_write_ts":null}
{"ts":"2021-11-18T08:00:21.486539","uids":["C4fKs01p1bdzLWvtQa"],"client_addr":"192.168.1.102","server_addr":"192.168.1.1","mac":"00:0b:db:63:58:a6","host_name":"m57-jo","client_fqdn":"m57-jo.","domain":"m57.biz","requested_addr":null,"assigned_addr":"192.168.1.102","lease_time":"59.4m","client_message":null,"server_message":null,"msg_types":["REQUEST","ACK"],"duration":"163.82ms","trans_id":null,"_write_ts":null}
{"ts":"2021-11-17T22:01:49.352035","uid":"C9cmwh4azI11sxrdR3","id.orig_h":"185.180.143.77","id.orig_p":44805,"id.resp_h":"198.71.247.91","id.resp_p":88,"id.vlan":null,"id.vlan_inner":null,"request_type":"AS","client":"/NM","service":"krbtgt/NM","success":null,"error_msg":null,"from":null,"till":"1970-01-01T00:00:00.000000","cipher":null,"forwardable":true,"renewable":true,"client_cert_subject":null,"client_cert_fuid":null,"server_cert_subject":null,"server_cert_fuid":null}
{"ts":"2021-11-18T08:05:09.134638","uid":"Cwk5in34AvxJ8MurDh","id":{"orig_h":"10.2.9.133","orig_p":49768,"resp_h":"10.2.9.9","resp_p":135},"rtt":"254.0us","named_pipe":"135","endpoint":"epmapper","operation":"ept_map","_write_ts":null}
[20:39:12.647] json-writer processed 33 events at a rate of 2675 events/sec in 12.34ms
```

Here we see all the variety of our data. This can be a bit overwhelming.

### Filter and reshape data

There are many other ways to slice and dice the data. For example, we could pick
a single schema:

```bash
vast export json '#type == "suricata.alert" | head 3'
```

```json
{"timestamp": "2021-11-17T13:49:34.301057", "flow_id": 2006383376373761, "pcap_cnt": 123, "vlan": null, "in_iface": null, "src_ip": "8.218.64.104", "src_port": 8000, "dest_ip": "198.71.247.91", "dest_port": 1027, "proto": "UDP", "event_type": "alert", "community_id": null, "alert": {"app_proto": null, "action": "allowed", "gid": 1, "signature_id": 2200075, "rev": 2, "signature": "SURICATA UDPv4 invalid checksum", "category": "Generic Protocol Command Decode", "severity": 3, "source": {"ip": null, "port": null}, "target": {"ip": null, "port": null}}, "flow": {"pkts_toserver": 1, "pkts_toclient": 0, "bytes_toserver": 49, "bytes_toclient": 0, "start": "2021-11-17T13:49:34.301057", "end": null, "age": null, "state": null, "reason": null, "alerted": null}, "payload": null, "payload_printable": null, "stream": null, "packet": null, "packet_info": {"linktype": null}}
{"timestamp": "2021-11-17T13:52:05.695469", "flow_id": 1868285155318879, "pcap_cnt": 143, "vlan": null, "in_iface": null, "src_ip": "14.1.112.177", "src_port": 38376, "dest_ip": "198.71.247.91", "dest_port": 123, "proto": "UDP", "event_type": "alert", "community_id": null, "alert": {"app_proto": null, "action": "allowed", "gid": 1, "signature_id": 2017919, "rev": 2, "signature": "ET DOS Possible NTP DDoS Inbound Frequent Un-Authed MON_LIST Requests IMPL 0x03", "category": "Attempted Denial of Service", "severity": 2, "source": {"ip": null, "port": null}, "target": {"ip": null, "port": null}}, "flow": {"pkts_toserver": 2, "pkts_toclient": 0, "bytes_toserver": 468, "bytes_toclient": 0, "start": "2021-11-17T13:52:05.695391", "end": null, "age": null, "state": null, "reason": null, "alerted": null}, "payload": null, "payload_printable": null, "stream": null, "packet": null, "packet_info": {"linktype": null}}
{"timestamp": "2021-11-17T14:39:24.485595", "flow_id": 368772671891675, "pcap_cnt": 723, "vlan": null, "in_iface": null, "src_ip": "167.94.138.20", "src_port": 36086, "dest_ip": "198.71.247.91", "dest_port": 5683, "proto": "UDP", "event_type": "alert", "community_id": null, "alert": {"app_proto": null, "action": "allowed", "gid": 1, "signature_id": 2200075, "rev": 2, "signature": "SURICATA UDPv4 invalid checksum", "category": "Generic Protocol Command Decode", "severity": 3, "source": {"ip": null, "port": null}, "target": {"ip": null, "port": null}}, "flow": {"pkts_toserver": 1, "pkts_toclient": 0, "bytes_toserver": 45, "bytes_toclient": 0, "start": "2021-11-17T14:39:24.485595", "end": null, "age": null, "state": null, "reason": null, "alerted": null}, "payload": null, "payload_printable": null, "stream": null, "packet": null, "packet_info": {"linktype": null}}
```

There are a lot of `null` values in there. We can filter them out by passing
`--omit-nulls` to the `json` printer:

```bash
vast export json --omit-nulls '#type == "suricata.alert" | head 3'
```

```json
{"timestamp": "2021-11-17T13:49:34.301057", "flow_id": 2006383376373761, "pcap_cnt": 123, "src_ip": "8.218.64.104", "src_port": 8000, "dest_ip": "198.71.247.91", "dest_port": 1027, "proto": "UDP", "event_type": "alert", "alert": {"action": "allowed", "gid": 1, "signature_id": 2200075, "rev": 2, "signature": "SURICATA UDPv4 invalid checksum", "category": "Generic Protocol Command Decode", "severity": 3, "source": {}, "target": {}}, "flow": {"pkts_toserver": 1, "pkts_toclient": 0, "bytes_toserver": 49, "bytes_toclient": 0, "start": "2021-11-17T13:49:34.301057"}, "packet_info": {}}
{"timestamp": "2021-11-17T13:52:05.695469", "flow_id": 1868285155318879, "pcap_cnt": 143, "src_ip": "14.1.112.177", "src_port": 38376, "dest_ip": "198.71.247.91", "dest_port": 123, "proto": "UDP", "event_type": "alert", "alert": {"action": "allowed", "gid": 1, "signature_id": 2017919, "rev": 2, "signature": "ET DOS Possible NTP DDoS Inbound Frequent Un-Authed MON_LIST Requests IMPL 0x03", "category": "Attempted Denial of Service", "severity": 2, "source": {}, "target": {}}, "flow": {"pkts_toserver": 2, "pkts_toclient": 0, "bytes_toserver": 468, "bytes_toclient": 0, "start": "2021-11-17T13:52:05.695391"}, "packet_info": {}}
{"timestamp": "2021-11-17T14:39:24.485595", "flow_id": 368772671891675, "pcap_cnt": 723, "src_ip": "167.94.138.20", "src_port": 36086, "dest_ip": "198.71.247.91", "dest_port": 5683, "proto": "UDP", "event_type": "alert", "alert": {"action": "allowed", "gid": 1, "signature_id": 2200075, "rev": 2, "signature": "SURICATA UDPv4 invalid checksum", "category": "Generic Protocol Command Decode", "severity": 3, "source": {}, "target": {}}, "flow": {"pkts_toserver": 1, "pkts_toclient": 0, "bytes_toserver": 45, "bytes_toclient": 0, "start": "2021-11-17T14:39:24.485595"}, "packet_info": {}}
```

Certainly less noisy. The
[`select`](../understand/operators/transformations/select.md) operator helps
selecting fields of interest:

```bash
vast export json '#type == "suricata.alert" | select src_ip, dest_ip, severity, signature | head 3'
```

```json
{"src_ip": "8.218.64.104", "dest_ip": "198.71.247.91", "alert": {"signature": "SURICATA UDPv4 invalid checksum", "severity": 3}}
{"src_ip": "14.1.112.177", "dest_ip": "198.71.247.91", "alert": {"signature": "ET DOS Possible NTP DDoS Inbound Frequent Un-Authed MON_LIST Requests IMPL 0x03", "severity": 2}}
{"src_ip": "167.94.138.20", "dest_ip": "198.71.247.91", "alert": {"signature": "SURICATA UDPv4 invalid checksum", "severity": 3}}`
```

### Summarize data with aggregations

Looking at the output, we see multiple alert severities. Let's understand their
distribution:

```bash
vast export json '#type == "suricata.alert" | summarize count=count(src_ip) by severity'
```

```json
{"alert.severity": 1, "count": 134644}
{"alert.severity": 2, "count": 26780}
{"alert.severity": 3, "count": 179713}
```

:::caution sort
This is the point where you'd typically do ad-hoc rollups (aka. "stack
counting") to extract the top-most values in your distribution. Unix folks know
this `... | sort | uniq -c | sort -n` pattern.

As of v3.0, VAST doesn't ship with the `sort` operator yet. Please bear with us,
[we scheduled it](https://github.com/tenzir/public-roadmap/issues/18) along with
various other pipeline operators.
:::

Suricata alerts with lower severity are more critical, with severity 1 being the
highest. We could further slice by signature type and check for some that
contain the string `SHELLCODE`:

```bash
vast export json 'severity == 1 | summarize count=count(src_ip) by signature | where /.*SHELLCODE.*/'
```

```json
{"alert.signature": "ET SHELLCODE Possible Call with No Offset TCP Shellcode", "count": 2}
{"alert.signature": "ET SHELLCODE Possible %41%41%41%41 Heap Spray Attempt", "count": 32}
```

Observe that we started the query with `severity == 1` and didn't include a type
filter. This is because VAST performs *suffix matching* on fields to make it
easy to specify fields in nested records. The fully qualified field name is
`suricata.alert.severity`, which is a bit unwieldy to type.

:::info Extractors
Aside from using field names, VAST offers powerful
[extractors](../understand/expressions.md#extractors) locating data.
If you don't know a field name, you can go through the type system, e.g., to
apply a query over all fields of the `ip` type by writing `:ip == 172.17.2.163`.

The left-hand side of this predicate is a *type extractor*, denoted by `:T` for
a builtin or user-defined type `T`. The right-hand side is an IP address literal
`172.17.2.163`. You can go one step further and just write `172.17.2.163` as
query. VAST infers the literal type and make a predicate out of it, i.e.,. `x`,
expands to `:T == x` where `T` is the type of `x`. Under the hood, the predicate
all possible fields with type address and yields a giant logical OR.

Similarly, you can write simply `:ip` to construct a query that returns any
record that has a field of type `ip`.
:::

We used a short form of a type extractor earlier when searching via `where
/.*SHELLCODE.*/`. Let's do this for IP addresses. And let's also pick something
different from JSON output, to showcase that it's not hard to change the last
step of rendering data:

```bash
vast export ascii '172.17.2.163 | head 10'
```

```
<2021-11-18T08:32:37.014285, "Ci7rxuICTjwjEy0Je", 172.17.2.163, 62865, 172.17.2.17, 445, nil, nil, "\\TRASHYHOUSES-DC.trashyhouses.com\IPC$", nil, nil, "PIPE">
<2021-11-18T08:47:37.024079, "CQ9OBn2APHOKaEV5Ti", 172.17.2.163, 62948, 172.17.2.17, 445, nil, nil, "\\TRASHYHOUSES-DC.trashyhouses.com\IPC$", nil, nil, "PIPE">
<2021-11-18T09:02:37.024756, "CxyRjb4BG8XPi6J7kj", 172.17.2.163, 63030, 172.17.2.17, 445, nil, nil, "\\TRASHYHOUSES-DC.trashyhouses.com\IPC$", nil, nil, "PIPE">
<2021-11-18T09:17:37.029355, "CRB5mhDCdhtLkuLwd", 172.17.2.163, 63124, 172.17.2.17, 445, nil, nil, "\\TRASHYHOUSES-DC.trashyhouses.com\IPC$", nil, nil, "PIPE">
<2021-11-18T09:32:37.035984, "CfY2244PsSPH55zLl9", 172.17.2.163, 63231, 172.17.2.17, 445, nil, nil, "\\TRASHYHOUSES-DC.trashyhouses.com\IPC$", nil, nil, "PIPE">
<2021-11-18T09:47:37.033750, "CH0HKu3LGfDOPIVm89", 172.17.2.163, 63337, 172.17.2.17, 445, nil, nil, "\\TRASHYHOUSES-DC.trashyhouses.com\IPC$", nil, nil, "PIPE">
<2021-11-18T10:02:37.032167, "C12s1O3dUOZIVTicl4", 172.17.2.163, 63413, 172.17.2.17, 445, nil, nil, "\\TRASHYHOUSES-DC.trashyhouses.com\IPC$", nil, nil, "PIPE">
<2021-11-18T10:04:12.630834, "CpZ1k21HG2mp7b5PFe", 172.17.2.163, 63426, 172.17.2.17, 445, nil, nil, "\\TRASHYHOUSES-DC.trashyhouses.com\sysvol", nil, nil, "DISK">
<2021-11-18T10:17:37.036408, "CE1K3a4QgJXmmyCYJ4", 172.17.2.163, 63481, 172.17.2.17, 445, nil, nil, "\\TRASHYHOUSES-DC.trashyhouses.com\IPC$", nil, nil, "PIPE">
<2021-11-18T10:32:37.045171, "CZKTgq2jm6IGsHyEQ2", 172.17.2.163, 63536, 172.17.2.17, 445, nil, nil, "\\TRASHYHOUSES-DC.trashyhouses.com\IPC$", nil, nil, "PIPE">
```

The `ascii` format displays the raw data without field names, for experiencing
maximum data density.

### Extract data with rich expressions

Finally, let's get a feel for the [expression
language](../understand/expressions.md). VAST comes with native types for IP
addresses, subnets, timestamps, and durations. These come in handy to succinctly
describe what you want:

```bash
vast export json '10.10.5.0/25 && (orig_bytes > 1 Mi || duration > 30 min) | select orig_h, resp_h, orig_bytes'
```

```json
{"id": {"orig_h": "10.10.5.101", "resp_h": "87.120.8.190"}, "orig_bytes": 1394538}
{"id": {"orig_h": "10.10.5.101", "resp_h": "87.120.8.190"}, "orig_bytes": 1550710}
{"id": {"orig_h": "10.10.5.101", "resp_h": "87.120.8.190"}, "orig_bytes": 565}
```

The above example extracts connections that either have sent more than 1 MiB or
lasted longer than 30 minutes. The value `10.10.5.0/25` actually expands to the
expression `:ip in 10.10.5.0/25 || :subnet == 10.10.5.0/25` under the hood,
meaning, VAST looks for any IP address field and performs a top-k prefix search
(by masking the top 25 bits IP address bits), or any subnet field where the value
matches exactly.

## Going deeper

This was just a brief summary of how you could sift through the data. Take a
look at various [operators](../understand/operators/README.md) VAST has to
offer and start writing pipelines!
