---
title: "Remove the `use_simple_format` option for `/serve`"
type: change
authors: dominiklohmann
pr: 4411
---

The `/serve` endpoint now always uses the simple output format for schema
definitions. The option `use_simple_format` is now ignored.
