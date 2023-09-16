The `decapsulate` operator no longer drops the PCAP packet data in incoming
events. To restore the old behavior, use `decapsulate | drop pcap` explicitly.
