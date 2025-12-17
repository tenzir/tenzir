---
title: "Update xxHash and hashing APIs"
type: change
author: mavam
created: 2021-10-22T19:09:40Z
pr: 1905
---

VAST no longer vendors [xxHash](https://github.com/Cyan4973/xxHash), which is
now a regular required dependency. Internally, VAST switched its default hash
function to XXH3, providing a speedup of up to 3x.
