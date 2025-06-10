---
title: "Switch default TCP port to 5158"
type: change
authors: tobim
pr: 2998
---

From now on VAST will use TCP port 5158 for its native inter process
communication. This change avoids collisions from dynamic port allocation on
Linux systems.
