---
title: "Don't allow field extractors to match field name suffixes"
type: bugfix
author: lava
created: 2021-03-18T10:29:23Z
pr: 1447
---

A query for a field or field name suffix that matches multiple fields of
different types would erroneously return no results.
