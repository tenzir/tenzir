---
title: "Make it possible to run VAST without user configs"
type: change
authors: dominiklohmann
pr: 1557
---

The new option `--disable-default-config-dirs` disables the loading of user and
system configuration, schema, and plugin directories. We use this option
internally when running integration tests.
