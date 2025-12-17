---
title: "Make it possible to run VAST without user configs"
type: change
author: dominiklohmann
created: 2021-04-20T08:31:46Z
pr: 1557
---

The new option `--disable-default-config-dirs` disables the loading of user and
system configuration, schema, and plugin directories. We use this option
internally when running integration tests.
