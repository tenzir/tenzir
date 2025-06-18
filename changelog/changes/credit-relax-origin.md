---
title: "SNI support in `from_http`"
type: bugfix
authors: tobim
pr: 5288
---

The `from_http` operator now correctly sets the domain for TLS SNI (Server Name Indication).
