---
title: "PRs 4716-4807"
type: feature
author: raxyte
created: 2024-11-11T12:20:55Z
pr: 4716
---

The following operators are now available in TQL2 for loading and
saving: `load_amqp`, `save_amqp`, `load_ftp`, `save_ftp`, `load_nic`,
`load_s3`, `save_s3`, `load_sqs`, `save_sqs`, `load_udp`, `save_udp`,
`load_zmq`, `save_zmq`, `save_tcp` and `save_email`.

The following new operators are available in TQL2 to convert event
streams to byte streams in various formats: `write_csv`, `write_feather`,
`write_json`, `write_lines`, `write_ndjson`, `write_parquet`, `write_pcap`, `write_ssv`, `write_tsv`,
`write_xsv`, `write_yaml`, `write_zeek_tsv`.
