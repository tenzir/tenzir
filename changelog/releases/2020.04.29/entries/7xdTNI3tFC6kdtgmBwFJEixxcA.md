---
title: "Use line reader timeout"
type: bugfix
author: dominiklohmann
created: 2020-04-23T09:33:20Z
pr: 835
---

Fixed a bug that could cause stalled input streams not to forward events to the
index and archive components for the JSON, CSV, and Syslog readers, when the
input stopped arriving but no EOF was sent. This is a follow-up to
[#750](https://github.com/tenzir/vast/pull/750). A timeout now ensures that that
the readers continue when some events were already handled, but the input
appears to be stalled.
