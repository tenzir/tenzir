---
title: "Pipeline execution under high loads"
type: bugfix
authors: jachris
pr: 5486
---

Previously, the execution of certain pipelines under high load scenarios could
lead to general unresponsiveness. In extreme cases, this meant that the platform
wasn't able to reach the node. This issue has now been resolved, leading to a
more reliable and responsive experience.
