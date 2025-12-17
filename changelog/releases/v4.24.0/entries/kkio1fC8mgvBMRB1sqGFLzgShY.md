---
title: "Make `to_hive` a \"local\" operator"
type: bugfix
author: dominiklohmann
created: 2024-11-14T22:19:34Z
pr: 4771
---

The `to_hive` operator now correctly writes files relative to the working
directory of a `tenzir` client process instead of relative to the node.
