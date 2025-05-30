---
title: "Use separate volumes in docker-compose"
type: bugfix
authors: sunnewehr
pr: 4749
---

The `docker compose` setup now uses separate local volumes for each `tenzir` directory. This fixes a bug where restarting the container resets installed packages or pipelines.
