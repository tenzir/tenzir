---
title: "Allow tuple-style syntax for parsing records"
type: feature
author: tobim
created: 2020-11-05T16:29:44Z
pr: 1129
---

The expression language now accepts records without field names. For example,`id
== <192.168.0.1, 41824, 143.51.53.13, 25, "tcp">` is now valid syntax and
instantiates a record with 5 fields. Note: expressions with records currently do
not execute.
