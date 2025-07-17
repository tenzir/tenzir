---
title: "Rename package artifacts from vast to tenzir"
type: change
authors: tobim
pr: 3203
---

The Debian package for Tenzir replaces previous VAST installations and attempts
to migrate existing data from VAST to Tenzir in the process. You can opt-out of
this migration by creating the file `/var/lib/vast/disable-migration`.
