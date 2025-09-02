---
title: "`insert_separator` option for `load_zmq`"
type: feature
authors: raxyte
pr: 5456
---

The `load_zmq` operator now supports an optional `insert_separator` parameter to append
a custom string to each received ZeroMQ message. This enables better message
separation and parsing for downstream operators.
