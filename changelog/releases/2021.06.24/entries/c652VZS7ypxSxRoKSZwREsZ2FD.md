---
title: "Handle arbitrary types in bloom filter synopsis"
type: bugfix
author: lava
created: 2021-05-27T16:24:08Z
pr: 1685
---

VAST no longer crashes when querying for string fields with non-string
values. Instead, an error message warns the user about an invalid query.
