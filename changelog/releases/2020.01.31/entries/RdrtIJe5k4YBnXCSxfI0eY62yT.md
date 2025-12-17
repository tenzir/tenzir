---
title: "Fix race in index lookup"
type: bugfix
author: tobim
created: 2020-01-08T18:57:03Z
pr: 703
---

A race condition in the index logic was able to lead to incomplete or empty
result sets for `vast export`.
