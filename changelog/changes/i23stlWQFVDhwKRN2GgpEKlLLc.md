---
title: "Support `show nics` to see network interfaces"
type: feature
authors: mavam
pr: 3517
---

You can now write `show nics` to get a list of network interfaces. Use `show
nics | select name` to a get a list of possible interface names for `from nic`.
