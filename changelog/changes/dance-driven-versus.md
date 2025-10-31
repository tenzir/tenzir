---
title: "More permissive syslog parsing"
type: change
authors: jachris
pr: 5541
---

The parser for RFC 5424 syslog messages now accepts structured data that
slightly diverges from the RFC 5424 specification. In particular, quotes and
other special characters are allowed in values unless it's ambiguous where they
belong to.
