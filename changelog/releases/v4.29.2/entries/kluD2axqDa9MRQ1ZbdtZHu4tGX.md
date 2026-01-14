---
title: "Change how signed ints are rendered in logs"
type: bugfix
author: dominiklohmann
created: 2025-03-05T08:59:43Z
pr: 5037
---

We fixed a bug in the `from_fluent_bit` and `to_fluent_bit` operators that
caused positive integer options to be forwarded with a leading `+`. For example,
`options={port: 9200}` forwarded the option `port=+9200` to Fluent Bit.
