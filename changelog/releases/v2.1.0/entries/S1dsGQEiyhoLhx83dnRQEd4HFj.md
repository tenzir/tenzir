---
title: "Parse time from JSON strings containing numbers"
type: bugfix
author: dominiklohmann
created: 2022-06-10T15:42:30Z
pr: 2340
---

The JSON import now treats `time` and `duration` fields correctly for JSON
strings containing a number, i.e., the JSON string `"1654735756"` now behaves
just like the JSON number `1654735756` and for a `time` field results in the
value `2022-06-09T00:49:16.000Z`.
