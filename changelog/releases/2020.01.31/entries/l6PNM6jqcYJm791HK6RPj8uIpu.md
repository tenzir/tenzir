---
title: "Add FreeBSD rc.d script"
type: feature
author: mavam
created: 2020-01-06T12:13:45Z
pr: 693
---

On FreeBSD, a VAST installation now includes an rc.d script that simpliefies
spinning up a VAST node. CMake installs the script at `PREFIX/etc/rc.d/vast`.
