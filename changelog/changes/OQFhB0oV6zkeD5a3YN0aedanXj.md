---
title: "Add options to omit empty values when exporting as JSON"
type: feature
authors: dominiklohmann
pr: 2856
---

The JSON export format gained the options `--omit-empty-records`,
`--omit-empty-lists`, and `--omit-empty-maps`, which cause empty records, lists,
and maps not to be rendered respectively. The options may be combined together
with the existing `--omit-nulls` option. Use `--omit-empty` to set all four
flags at once.
