---
title: Make `to_hive` a "local" operator
type: bugfix
authors: dominiklohmann
pr: 4771
---

The `to_hive` operator now correctly writes files relative to the working
directory of a `tenzir` client process instead of relative to the node.
