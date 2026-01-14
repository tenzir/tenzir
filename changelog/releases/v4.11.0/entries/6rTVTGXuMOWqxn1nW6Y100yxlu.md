---
title: "Add SQS Connector"
type: bugfix
author: mavam
created: 2024-03-21T17:16:38Z
pr: 3819
---

Source operators that do not quit on their own only freed their resources after
they had emitted an additional output, even after the pipeline had already
exited. This sometimes caused errors when restarting pipelines, and in rare
cases caused Tenzir nodes to hang on shutdown. This no longer happens, and the
entire pipeline shuts down at once.
