---
title: "Fix a hang on shutdown and remove deprecated things"
type: bugfix
authors: dominiklohmann
pr: 4187
---

Startup failures caused by invalid pipelines or contexts deployed as code in the
configuration file sometimes caused the node to hang instead of shutting down
with an error message. The node now shuts down as expected when this happens.
