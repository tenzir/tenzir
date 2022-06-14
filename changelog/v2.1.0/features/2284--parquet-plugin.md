A new parquet store plugin allows VAST to store its data as parquet files,
increasing storage efficiency at the expense of higher deserialization costs.
Storage requirement for the archive (excluding indices) is reduced by another
10-20% compared to the existing segment store with Zstd compression enabled.
CPU usage for suricata import is up ~ 10%,  mostly related to the more
expensive serialization.
