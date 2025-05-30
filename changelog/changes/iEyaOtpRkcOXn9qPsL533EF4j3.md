---
title: "Fix TLS options in `from_http`"
type: bugfix
authors: raxyte
pr: 5135
---

We fixed a bug in parsing the TLS options for the `from_http` operator,
preventing disabling of TLS.
