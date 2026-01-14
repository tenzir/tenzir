---
title: "Switch default TCP port to 5158"
type: change
author: tobim
created: 2023-03-08T16:59:42Z
pr: 2998
---

From now on VAST will use TCP port 5158 for its native inter process
communication. This change avoids collisions from dynamic port allocation on
Linux systems.
