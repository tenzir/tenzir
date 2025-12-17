---
title: "Support setting profiles in the Velociraptor config"
type: feature
author: dominiklohmann
created: 2024-01-23T13:30:32Z
pr: 3848
---

The `velociraptor` operator gained a new `--profile <profile>` option to support
multiple configured Velociraptor instances. To opt into using profiles, move
your Velociraptor configuration in `<configdir>/tenzir/plugin/velociraptor.yaml`
from `<config>` to `profiles.<profile>.<config>`.
