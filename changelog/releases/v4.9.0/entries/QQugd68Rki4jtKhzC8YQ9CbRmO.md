---
title: "Allow plugins to bundle further builtins"
type: change
author: dominiklohmann
created: 2024-01-31T18:48:38Z
pr: 3877
---

Plugins may now depend on other plugins. Plugins with unmet dependencies are
automatically disabled. For example, the `lookup` and `enrich` plugins now
depend on the `context` plugin. Run `show plugins` to see all available plugins
and their dependencies.
