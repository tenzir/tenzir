---
title: "`insert_separator` option for `load_zmq`"
type: feature
author: raxyte
created: 2025-09-02T14:12:05Z
pr: 5456
---

The `load_zmq` operator now supports an optional `insert_separator` parameter to append
a custom string to each received ZeroMQ message. This enables better message
separation and parsing for downstream operators.
