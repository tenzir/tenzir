---
title: "Don't allow field extractors to match field name suffixes"
type: bugfix
authors: lava
pr: 1447
---

A query for a field or field name suffix that matches multiple fields of
different types would erroneously return no results.
