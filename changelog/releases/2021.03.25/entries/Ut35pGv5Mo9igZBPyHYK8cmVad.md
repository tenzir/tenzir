---
title: "Use non-throwing std::filesystem functions in the type registry"
type: bugfix
author: tobim
created: 2021-03-18T20:29:43Z
pr: 1472
---

Insufficient permissions for one of the paths in the `schema-dirs` option would
lead to a crash in `vast start`.
