---
title: "Fix TLS options in `from_http`"
type: bugfix
author: raxyte
created: 2025-04-24T13:08:22Z
pr: 5135
---

We fixed a bug in parsing the TLS options for the `from_http` operator,
preventing disabling of TLS.
