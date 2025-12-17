---
title: "Support IP-in-subnet queries in lookup tables"
type: feature
author: mavam
created: 2024-05-30T06:58:47Z
pr: 4051
---

The `lookup-table` context now performs longest-prefix matches when the table
key is of type `subnet` and the to-be-enriched field of type `ip`. For example,
a lookup table with key `10.0.0.0/8` will match when enriching the IP address
`10.1.1.1`.
