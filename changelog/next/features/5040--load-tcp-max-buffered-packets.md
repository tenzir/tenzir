The newly added `max_buffered_packets` for `load_tcp` controls how many reads
the operator schedules in advance on the socket. The option defaults to 10.
