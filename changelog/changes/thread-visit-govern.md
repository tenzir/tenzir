---
title: "Removed warning for `void` metrics"
type: bugfix
authors: jachris
pr: 5598
---

The non-actionable warning "received an operator metric without a unit" that was
sometimes emitted for closed subpipelines was removed.
