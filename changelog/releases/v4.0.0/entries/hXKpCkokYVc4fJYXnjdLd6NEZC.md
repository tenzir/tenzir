---
title: "Remove the `prefix()` function from the REST endpoint plugin API"
type: change
author: lava
created: 2023-06-14T17:57:13Z
pr: 3221
---

We removed the `rest_endpoint_plugin::prefix()` function from
the public API of the `rest_endpoint_plugin` class. For a migration,
existing users should prepend the prefix manually to all endpoints
defined by their plugin.
