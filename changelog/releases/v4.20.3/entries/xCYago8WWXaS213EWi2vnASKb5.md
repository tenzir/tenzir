---
title: "Remove the delay between importing and exporting events"
type: bugfix
author: dominiklohmann
created: 2024-09-09T07:46:31Z
pr: 4583
---

We fixed a bug where the `export`, `metrics`, and `diagnostics` operators were
sometimes missing events from up to the last 30 seconds. In the Tenzir Platform,
this showed itself as a gap in activity sparkbars upon loading the page.
