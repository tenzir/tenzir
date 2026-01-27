---
title: "Split out `name` option and use metadata server when unset"
type: bugfix
author: raxyte
created: 2025-04-30T11:29:49Z
pr: 5160
---

The `to_google_cloud_logging` operator is now available in both Docker and static
builds. The operator had earlier been missing due to a configuration issue.
