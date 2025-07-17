---
title: "Remove wrong EXPOSE in Dockerfile"
type: bugfix
authors: dominiklohmann
pr: 4099
---

Tenzir Docker images no longer expose 5158/tcp by default, as this prevented
running multiple containers in the same network or in host mode.
