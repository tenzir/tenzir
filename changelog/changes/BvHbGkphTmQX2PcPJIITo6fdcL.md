---
title: "Do not persist pipeline state updates on node shutdown"
type: bugfix
authors: dominiklohmann
pr: 4261
---

Some *Running* pipelines were considered *Completed* when the node shut down,
causing them not to start up again automatically when the node restarted. Now,
the node only considers pipelines *Completed* that entered the state on their
own before the node's shutdown.
