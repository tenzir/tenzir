---
title: "Implement support for `:string == /pattern/` queries"
type: feature
author: dominiklohmann
created: 2022-12-09T20:37:19Z
pr: 2769
---

Queries of the forms `:string == /pattern/`, `field == /pattern/`, `#type ==
/pattern/`, and their respective negations now work as expected.
