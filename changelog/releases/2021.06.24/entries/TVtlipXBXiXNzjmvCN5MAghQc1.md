---
title: "Rework the plugin loading logic"
type: feature
author: dominiklohmann
created: 2021-06-08T08:52:27Z
pr: 1703
---

The options `vast.plugins` and `vast.plugin-dirs` may now be specified on the
command line as well as the configuration. Use the options `--plugins` and
`--plugin-dirs` respectively.

Add the reserved plugin name `bundled` to `vast.plugins` to enable load all
bundled plugins, i.e., static or dynamic plugins built alongside VAST, or use
`--plugins=bundled` on the command line. The reserved plugin name `all` causes
all bundled and external plugins to be loaded, i.e., all shared libraries
matching `libvast-plugin-*` from the configured `vast.plugin-dirs`.
