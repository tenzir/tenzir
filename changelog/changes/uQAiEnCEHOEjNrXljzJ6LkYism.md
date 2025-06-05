---
title: "Age rotation for old data"
type: feature
authors: lava
pr: 1103
---

A new *disk monitor* component can now monitor the database size and delete data
that exceeds a specified threshold. Once VAST reaches the maximum amount of disk
space, the disk monitor deletes the oldest data. The command-line options
`--disk-quota-high`, `--disk-quota-low`, and `--disk-quota-check-interval`
control the rotation behavior.
