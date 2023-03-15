---
description: Open Source flow monitor
---

# Process Argus Flow Logs

[Argus](https://qosient.com/argus/index.shtml) is an open-source flow monitor
that computes a variety of connection statistics.

The UNIX tool `argus` processes either [PCAP](../understand/formats/pcap.md) or
[NetFlow](../understand/formats/netflow.md) data and generates binary output.
The companion utility `ra` transforms this binary output into a textual form
that VAST can parse.

Ingesting Argus data involves the following steps:

1. Read PCAP or NetFlow data with `argus`
2. Convert the binary Argus data into CSV with `ra`
3. Pipe the `ra` output to `vast`

## Read network data

To read a PCAP file, pass a file via `-r`:

```bash
argus -r trace
```

To read from standard input, use `-r -`. Similarly, to write to standard
output, use `-w -`.

## Convert Argus to CSV

Converting `argus` output to CSV requires the following flags:

- `-c ,` to enable CSV mode
- `-L0` to print a header with field names once
- `-n` suppress port nubmer to service conversions

The first column contains the timestamp, but unfortunately the default format
doesn't contain dates. Changing the timestamp format requires passing a
custom configuration file via `-F ra.conf` with the following contents:

```bash
RA_TIME_FORMAT="%y-%m-%d+%T.%f"
```

Finally, the `-s +a,b,c,...` flag includes list of field names that should be
appended after the default fields. Consult the manpage of `ra` under the `-s`
section for valid field names.

Put together, the following example generates valid CSV output for a PCAP file
called `trace.pcap`:

```bash
argus -r trace.pcap -w - |
  ra -F ra.conf -L0 -c , -n -s +spkts,dpkts,load,pcr
```

This generates the following output:

```csv
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

## Ingest Argus CSV output

Since VAST has [CSV support](../understand/formats/csv.md), ingesting Argus CSV
output only requires an adequate schema. VAST already ships with an argus schema
containing a type `argus.record` that covers all fields from the `ra` man page.

The following command imports a file `argus.csv`:

```bash
vast import -t argus.record csv < argus.csv
```

Alternatively, this UNIX pipe processes a PCAP trace without intermediate file
and ships the data directly to VAST:

```bash
argus -r trace.pcap -w - |
  ra -F ra.conf -L0 -c , -n -s +spkts,dpkts,load,pcr |
  vast import -t argus.record csv
```
