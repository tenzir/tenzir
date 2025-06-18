---
title: "Shut down node a configured pipeline fails to start"
type: bugfix
authors: dominiklohmann
pr: 4097
---

Nodes now shut down with a non-zero exit code when pipelines configured as part
of the `tenzir.yaml` file fail to start, making such configuration errors easier
to spot.
