---
title: "Change the syntax of the `slice` operator"
type: change
authors: dominiklohmann
pr: 4211
---

The `slice` operator now expects its arguments in the form `<begin>:<end>`,
where either the begin or the end value may be omitted. For example, `slice 10:`
returns all but the first 10 events, `slice 10:20` returns events 10 to 20
(exclusive), and `slice :-10` returns all but the last 10 events.
