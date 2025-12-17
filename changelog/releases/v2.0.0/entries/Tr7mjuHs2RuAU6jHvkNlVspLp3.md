---
title: "Load stores lazily"
type: bugfix
author: dominiklohmann
created: 2022-03-14T11:06:38Z
pr: 2146
---

The `count --estimate` erroneously materialized store files from disk,
resulting in an unneeded performance penalty. VAST now answers approximate
count queries by solely consulting the relevant index files.
