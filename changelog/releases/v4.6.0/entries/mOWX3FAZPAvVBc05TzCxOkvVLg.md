---
title: "Add file extension detection to `from`/`to`"
type: feature
author: eliaskosunen
created: 2023-11-27T13:29:15Z
pr: 3653
---

When using `from <URL>` and `to <URL>` without specifying the format explicitly
using a `read`/`write` argument, the default format is determined by the file
extension for all loaders and savers, if possible. Previously, that was only
done when using the `file` loader/saver. Additionally, if the file name would
indicate some sort of compression (e.g. `.gz`), compression and decompression is
performed automatically. For example, `from https://example.com/myfile.yml.gz`
is expanded to `load https://example.com/myfile.yml.gz | decompress gzip | read
yaml` automatically.
