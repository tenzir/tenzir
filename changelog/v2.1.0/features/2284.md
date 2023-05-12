A new parquet store plugin allows VAST to store its data as parquet files,
increasing storage efficiency at the expense of higher deserialization costs.
Storage requirements for the VAST database is reduced by another
15-20% compared to the existing segment store with Zstd compression enabled.
CPU usage for suricata import is up ~ 10%,  mostly related to the more
expensive serialization.
Deserialization (reading) of a partition is significantly more expensive,
increasing CPU utilization by about 100%, and should be carefully considered
and compared to the potential reduction in storage cost and I/O operations.
