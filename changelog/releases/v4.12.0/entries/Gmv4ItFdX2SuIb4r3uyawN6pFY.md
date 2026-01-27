---
title: "Remove wrong EXPOSE in Dockerfile"
type: bugfix
author: dominiklohmann
created: 2024-04-05T10:22:14Z
pr: 4099
---

Tenzir Docker images no longer expose 5158/tcp by default, as this prevented
running multiple containers in the same network or in host mode.
