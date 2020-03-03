The PCAP import format uses [libpcap](https://www.tcpdump.org) to read network
packets from a trace or an interface.

VAST automatically calculates the [Community
ID](https://github.com/corelight/community-id-spec) for PCAPs for better
pivoting support. The extra computation induces an overhead of approximately 15%
of the ingestion rate. The option `--disable-community-id` can be used to
disable the computation completely.
