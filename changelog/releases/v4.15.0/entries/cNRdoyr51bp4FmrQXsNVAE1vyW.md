---
title: "Add an option to `export` both past and future events"
type: feature
author: IyeOnline
created: 2024-05-31T11:13:42Z
pr: 4203
---

The `export`, `metrics`, and `diagnostics` operators now features a `--retro`
flag. This flag will make the operators first export past events, even when
`--live` is set. Specify both options explicitly to first return past events and
then immediately switch into live mode.
