# PCAP Plugin for VAST

The PCAP plugin for VAST adds the ability to import and export data in the PCAP
format to VAST.

## Import

The `import pcap` command uses [libpcap][libpcap] to read network packets from a
trace or an interface.

The `spawn source pcap` command spawns a PCAP source inside the node and is the
analog to the `import pcap` command.

VAST automatically calculates the [Community ID][community-id-spec] for PCAPs
for better pivoting support. The extra computation induces an overhead of
approximately 15% of the ingestion rate. The option `--disable-community-id`
disables the computation completely.

The PCAP import format has many additional options that offer a user interface
that should be familiar to users of other tools interacting with PCAPs. To see a
list of all available options, run `vast import pcap help`.

Here's an example that reads from the network interface `en0` cuts off packets
after 65535 bytes.

```bash
sudo vast import pcap --interface=en0 --cutoff=65535
```

## Export

The PCAP export format uses [libpcap][libpcap] to write PCAP events as a trace.

This command only supports events of type `pcap.packet`. As a result, VAST
transforms a provided query expression `E` into `#type == "pcap.packet" && E`.

[libpcap]: https://www.tcpdump.org
[community-id-spec]: https://github.com/corelight/community-id-spec
