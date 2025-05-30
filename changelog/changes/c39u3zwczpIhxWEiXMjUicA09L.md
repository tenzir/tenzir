---
title: "Improve rendering of error messages & fix record to map conversion"
type: change
authors: tobim
pr: 1842
---

Strings in error or warning log messages are no longer escaped, greatly
improving readability of messages containing nested error contexts.
