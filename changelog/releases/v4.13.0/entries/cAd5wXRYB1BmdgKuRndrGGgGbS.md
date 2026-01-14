---
title: "Fix a hang on shutdown and remove deprecated things"
type: bugfix
author: dominiklohmann
created: 2024-05-06T08:19:05Z
pr: 4187
---

Startup failures caused by invalid pipelines or contexts deployed as code in the
configuration file sometimes caused the node to hang instead of shutting down
with an error message. The node now shuts down as expected when this happens.
