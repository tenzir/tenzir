---
title: "Implement `to_google_cloud_logging`"
type: change
authors: raxyte
pr: 5135
---

The `file_contents` function now returns contents as `string` by default.
Non-UTF-8 files can be read by specifying the `binary=true` option.
