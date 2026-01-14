---
title: "Make schema definitions represent the type system exactly"
type: feature
author: dominiklohmann
created: 2025-03-25T16:02:14Z
pr: 5062
---

We introduced a new `type_of(x: any) -> record` function that returns the exact
type definition of a TQL expression. For example, `this = type_of(this)`
replaces an event with its schema's definition.

The `/serve` endpoint gained a new option `schema`, which can be set to `legacy`
(default), `exact`, or `never`. The `legacy` option causes the schema definition
to be rendered in a simplified way, which is the current default. The `exact`
option causes the schema definitions to be rendered exactly without omitting any
information. Set the option to `never` to omit schema definitions entirely.
