---
title: "Improve rendering of error messages & fix record to map conversion"
type: change
author: tobim
created: 2021-08-17T09:10:27Z
pr: 1842
---

Strings in error or warning log messages are no longer escaped, greatly
improving readability of messages containing nested error contexts.
