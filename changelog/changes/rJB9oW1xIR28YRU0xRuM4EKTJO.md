---
title: "Support setting profiles in the Velociraptor config"
type: feature
authors: dominiklohmann
pr: 3848
---

The `velociraptor` operator gained a new `--profile <profile>` option to support
multiple configured Velociraptor instances. To opt into using profiles, move
your Velociraptor configuration in `<configdir>/tenzir/plugin/velociraptor.yaml`
from `<config>` to `profiles.<profile>.<config>`.
