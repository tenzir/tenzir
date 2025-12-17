---
title: "Bundle an example configuration file with plugins"
type: feature
author: dominiklohmann
created: 2021-08-27T11:21:31Z
pr: 1860
---

If present in the plugin source directory, the build scaffolding now
automatically installs `<plugin>.yaml.example` files, commenting out every line
so the file has no effect. This serves as documentation for operators that can
modify the installed file in-place.
