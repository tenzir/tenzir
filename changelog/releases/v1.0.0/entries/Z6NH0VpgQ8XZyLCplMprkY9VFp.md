---
title: "Create projection plugin"
type: feature
author: 6yozo
created: 2021-12-17T11:12:51Z
pr: 2000
---

VAST has a new transform step: `project`, which keeps the fields with configured
key suffixes and removes the rest from the input. At the same time, the `delete`
transform step can remove not only one but multiple fields from the input based
on the configured key suffixes.
