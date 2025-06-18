---
title: "Update xxHash and hashing APIs"
type: bugfix
authors: mavam
pr: 1905
---

When reading IPv6 addresses from PCAP data, only the first 4 bytes have been
considered. VAST now stores all 16 bytes.
