---
title: "Improved Syslog Output Schema"
type: bugfix
authors: IyeOnline
pr: 5472
---

We have improved our `read_syslog` operator and `parse_syslog`
function. They no longer re-order fields if the syslog format
changes mid-stream and produce correctly typed null values for
the nillvalue `-`.
