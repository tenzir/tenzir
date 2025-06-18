---
title: "Terminate exporters when sinks die"
type: bugfix
authors: mavam
pr: 1006
---

When continuous query in a client process terminated, the node did not clean up
the corresponding server-side state. This memory leak no longer exists.
