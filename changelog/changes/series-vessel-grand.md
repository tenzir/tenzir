---
title: "Keep zeek TSV logs as-is in `read_zeek_tsv`"
type: change
authors: tobim
pr: 5461
---

Parsing Zeek TSV logs no longer attempts to cast the parsed events to a shipped Zeek schema.
