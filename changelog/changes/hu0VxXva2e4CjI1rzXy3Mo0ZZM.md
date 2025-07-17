---
title: "Split out `name` option and use metadata server when unset"
type: bugfix
authors: raxyte
pr: 5160
---

The `to_google_cloud_logging` operator is now available in both Docker and static
builds. The operator had earlier been missing due to a configuration issue.
