---
title: "Fixed `to_kafka` crash"
type: bugfix
authors: IyeOnline
pr: 5465
---

The recently released `to_kafka` operator would fail with an internal error
when used without specifying the `message` argument.

The operator now works as expected, sending the entire event if the argument
is not specified.
