---
title: "Fix passing string params to `{from,to}_fluent_bit`"
type: bugfix
author: dominiklohmann
created: 2025-03-17T15:57:51Z
pr: 5053
---

We fixed a regression that caused strings passed as options to the
`from_fluent_bit` and `to_fluent_bit` operators to incorrectly be surrounded by
double quotes.

`to_fluent_bit` incorrectly reported zero bytes being pushed to the Fluent Bit
engine as an error. This no longer happens.
