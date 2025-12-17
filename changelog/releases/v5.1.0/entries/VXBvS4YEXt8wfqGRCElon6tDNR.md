---
title: "Implement the `move` keyword"
type: feature
author: dominiklohmann
created: 2025-04-24T14:07:38Z
pr: 5127
---

The `move` keyword may be used in front of fields anywhere in assignments to
automatically drop fields after the assignment. For example, `foo = {bar: move
bar, baz: move baz}` moves the top-level fields `bar` and `baz` into a new
record under the top-level field `foo`.

The `move`, `drop`, and `unroll` operators now support the `?` field access
notation to suppress warnings when the accessed field does not exist or the
parent record is `null`. For example, `drop foo?` only drops the field `foo` if
it exists, and does not warn if it doesn't. This also works with the newly
introduced `move` keyword.
