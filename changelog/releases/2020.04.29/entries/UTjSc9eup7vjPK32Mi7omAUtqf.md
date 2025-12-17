---
title: "Make archive session extraction interruptible"
type: bugfix
author: dominiklohmann
created: 2020-04-09T16:45:23Z
pr: 825
---

Archive lookups are now interruptible. This change fixes an issue that caused
consecutive exports to slow down the node, which improves the overall
performance for larger databases considerably.
