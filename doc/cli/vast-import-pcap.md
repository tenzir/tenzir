The `import pcap` command uses [libpcap](https://www.tcpdump.org) to read
network packets from a trace or an interface.

VAST automatically calculates the [Community
ID](https://github.com/corelight/community-id-spec) for PCAPs for better
pivoting support. The extra computation induces an overhead of approximately 15%
of the ingestion rate. The option `--disable-community-id` disables the
computation completely.

The PCAP import format has many additional options that offer a user interface
that should be familiar to users of other tools interacting with PCAPs. To see
a list of all available options, run `vast import pcap help`.

Here's an example that reads from the network interface `en0` cuts off packets
after 65535 bytes.

```bash
sudo vast import pcap --interface=en0 --cutoff=65535
```
