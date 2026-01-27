---
title: "Support `show nics` to see network interfaces"
type: feature
author: mavam
created: 2023-09-18T08:31:23Z
pr: 3517
---

You can now write `show nics` to get a list of network interfaces. Use `show
nics | select name` to a get a list of possible interface names for `from nic`.
