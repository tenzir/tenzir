---
title: Kafka operators no longer crash at shutdown with `debug` logging
type: bugfix
authors:
  - zedoraps
created: 2026-11-12T16:00:00Z
---

Pipelines using `from_kafka` or `to_kafka` no longer crash with a segmentation
fault when they shut down while the librdkafka `debug` option is set, as in
`options={"debug": "generic"}`. The pipeline finished its work, but the process
then crashed while it tore down the Kafka client.

Setting `debug` also no longer floods the pipeline with warnings. Previously,
every librdkafka log line, including debug output, became a warning diagnostic
such as `warning: librdkafka debug: ...`. Now only librdkafka warnings and errors
become diagnostics. Less severe log lines go to the Tenzir log instead: debug
output at the `debug` level, and notices and informational messages at the
`verbose` level. The log also reports at the `verbose` level when a broker
throttles the client. To see the output of the `debug` option, raise the log
verbosity accordingly, for example with `--console-verbosity=debug`.
