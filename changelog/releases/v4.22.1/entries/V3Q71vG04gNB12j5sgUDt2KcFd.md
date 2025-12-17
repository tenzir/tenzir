---
title: "Ensure cache directory is writable for multiple users"
type: bugfix
author: dominiklohmann
created: 2024-10-22T17:39:09Z
pr: 4694
---

Using the `tenzir` process from multiple users on the same host sometimes failed
because the cache directory was not writable for all users. This no longer
happens.
