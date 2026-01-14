---
title: "Removed warning for `void` metrics"
type: bugfix
author: jachris
created: 2025-12-08T11:09:23Z
pr: 5598
---

The non-actionable warning "received an operator metric without a unit" that was
sometimes emitted for closed subpipelines was removed.
