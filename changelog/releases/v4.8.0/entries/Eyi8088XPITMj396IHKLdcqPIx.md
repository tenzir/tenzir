---
title: "Switch from JSON to MsgPack data transport"
type: change
author: mavam
created: 2024-01-09T18:06:35Z
pr: 3770
---

The `fluent-bit` source operator no longer performs JSON conversion from
Fluent Bit prior to processing an event. Instead, it directly processes the
MsgPack data that Fluent Bit uses internally for more robust and quicker event
delivery.
