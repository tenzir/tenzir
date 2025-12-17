---
title: "Expand CAF stream slot ids to 32 bits"
type: bugfix
author: lava
created: 2020-08-14T11:37:01Z
pr: 1020
---

When running VAST under heavy load, CAF stream slot ids could wrap around after
a few days and deadlock the system. As a workaround, we extended the slot id bit
width to make the time until this happens unrealistically large.
