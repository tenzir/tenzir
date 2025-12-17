---
title: "Optionally read environment variable for VAST endpoint"
type: feature
author: rolandpeelen
created: 2021-06-11T12:00:51Z
pr: 1714
---

It's now possible to configure the VAST endpoint as an environment variable
by setting `VAST_ENDPOINT`. This has higher precedence than setting
`vast.endpoint` in configuration files, but lower precedence than passing
`--endpoint=` on the command-line.
