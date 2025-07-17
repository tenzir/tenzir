---
title: "Make UDO resolution order-independent"
type: bugfix
authors: dominiklohmann
pr: 5029
---

The resolution of user-defined operator aliases in the `tenzir.operators`
section is no longer order-dependent. Previously, an operator `foo` may have
depended on an operator `bar`, but not the other way around. This limitation no
longer exists.
