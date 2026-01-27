---
title: "Fix error response and lifetime issues in `from_opensearch`"
type: bugfix
author: raxyte
created: 2025-04-02T15:02:07Z
pr: 5096
---

We fixed a bug that caused the `from_opensearch` operator to crash on high
volume input. Additionally, the operator now correctly responds to requests.
