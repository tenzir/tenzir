---
title: "`from_http server=true` assertion failures"
type: bugfix
author: raxyte
created: 2025-07-03T17:10:45Z
pr: 5325
---

`from_http server=true` failed with internal assertions and stopped the pipeline on
receiving requests when `metadata_field` was specified.
