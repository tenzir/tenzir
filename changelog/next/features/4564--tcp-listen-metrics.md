We added a metric for TCP connections. It emits one event per second for every
active connection and contains the number of reads and writes on the socket and
the number of bytes that were transmitted in that time frame.
