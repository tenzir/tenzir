---
title: "SNI support in `from_http`"
type: bugfix
author: tobim
created: 2025-06-18T12:11:35Z
pr: 5288
---

The `from_http` operator now correctly sets the domain for TLS SNI (Server Name Indication).
