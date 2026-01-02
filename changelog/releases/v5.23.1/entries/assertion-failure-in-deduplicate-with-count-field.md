---
title: Assertion failure in deduplicate with count_field
type: bugfix
author: raxyte
created: 2025-12-30T14:36:18.446457Z
---

The `deduplicate` operator with `count_field` option could cause assertion failures when discarding events.
