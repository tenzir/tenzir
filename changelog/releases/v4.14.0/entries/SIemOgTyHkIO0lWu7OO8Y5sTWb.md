---
title: "Make syslog parser more lenient"
type: bugfix
author: mavam
created: 2024-05-17T05:46:19Z
pr: 4225
---

The `syslog` parser incorrectly identified a message without hostname and tag as
one with hostname and no tag. This resulted in a hostname with a trailing colon,
e.g., `zscaler-nss:`. In such messages, the parser now correctly sets the
hostname to `null` and assigns `zscaler-nss` as tag/app, without the trailing
colon.
