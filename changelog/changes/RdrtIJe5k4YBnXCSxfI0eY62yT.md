---
title: "Fix race in index lookup"
type: bugfix
authors: tobim
pr: 703
---

A race condition in the index logic was able to lead to incomplete or empty
result sets for `vast export`.
