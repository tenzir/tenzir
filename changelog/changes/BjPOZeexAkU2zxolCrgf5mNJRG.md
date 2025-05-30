---
title: "Fix fluentbit engine stop logic and disable TLS default"
type: bugfix
authors: lava
pr: 5070
---

Fixed an error in the `{from,to}_fluent_bit` operators
that would cause it to fail to start successfully when
using an input plugin (in particular the `elasticsearch` plugin)
when the TLS setting was enabled without specifying a keyfile.
