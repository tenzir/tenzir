---
title: "Ensure cache directory is writable for multiple users"
type: bugfix
authors: dominiklohmann
pr: 4694
---

Using the `tenzir` process from multiple users on the same host sometimes failed
because the cache directory was not writable for all users. This no longer
happens.
