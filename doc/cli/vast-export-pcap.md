The PCAP export format uses [libpcap](https://www.tcpdump.org) to write PCAP
events as a trace.

This command only supports events of type `pcap.packet`. As a result, VAST
transforms a provided query expression `E` into `#type == "pcap.packet" && E`.
