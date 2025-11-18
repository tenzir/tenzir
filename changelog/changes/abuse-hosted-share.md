---
title: "Backpressure in `publish` and `subscribe`"
type: bugfix
authors: jachris
pr: 5568
---

Previously, the backpressure mechanism in `publish` and `subscribe` was not
working as intended. Thus, publishing pipelines continued processing data
even when downstream consumers were lagging far behind. This is now fixed. As a
result, memory consumption for pipelines connected by `publish` and `subscribe`
is reduced significantly in those cases.
