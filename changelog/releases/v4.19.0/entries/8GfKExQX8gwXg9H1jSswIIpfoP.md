---
title: "Remove the `use_simple_format` option for `/serve`"
type: change
author: dominiklohmann
created: 2024-07-22T14:22:46Z
pr: 4411
---

The `/serve` endpoint now always uses the simple output format for schema
definitions. The option `use_simple_format` is now ignored.
