---
title: "Add a package manager"
type: feature
author: lava
created: 2024-07-22T08:01:54Z
pr: 4344
---

The new `package` operator allows for adding and removing packages, a
combination of pipelines and contexts deployed to a node as a set. Nodes load
packages installed to `<configdir>/tenzir/package/<package-name>/package.yaml`
on startup.
