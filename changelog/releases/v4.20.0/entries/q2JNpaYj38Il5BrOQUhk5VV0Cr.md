---
title: "Remove special character escaping from `lines_printer`"
type: change
author: raxyte
created: 2024-08-26T11:45:20Z
pr: 4520
---

The `lines` printer now does not perform any escaping and is no longer an alias to
the `ssv` printer. Additionally, nulls are skipped, instead of being printed
as `-`.
