---
title: "Remove check whether config file is a regular file"
type: bugfix
author: dominiklohmann
created: 2020-12-18T13:23:04Z
pr: 1248
---

Manually specified configuration files may reside in the default location
directories. Configuration files can be symlinked.
