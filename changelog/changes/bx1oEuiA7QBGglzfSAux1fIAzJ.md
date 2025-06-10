---
title: "Remove check whether config file is a regular file"
type: bugfix
authors: dominiklohmann
pr: 1248
---

Manually specified configuration files may reside in the default location
directories. Configuration files can be symlinked.
