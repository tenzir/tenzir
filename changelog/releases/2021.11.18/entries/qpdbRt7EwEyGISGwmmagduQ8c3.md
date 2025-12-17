---
title: "Update xxHash and hashing APIs"
type: bugfix
author: mavam
created: 2021-10-22T19:09:40Z
pr: 1905
---

When reading IPv6 addresses from PCAP data, only the first 4 bytes have been
considered. VAST now stores all 16 bytes.
