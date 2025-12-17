---
title: "Improve `to_splunk` TLS functionality"
type: feature
author: raxyte
created: 2024-12-02T13:03:20Z
pr: 4825
---

The `to_splunk` operator now supports the `cacert`, `certfile`, and `keyfile`
options to provide certificates for the TLS connection.
