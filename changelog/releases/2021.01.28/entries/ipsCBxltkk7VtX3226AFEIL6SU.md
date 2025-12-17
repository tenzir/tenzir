---
title: "PRs 1208-1264-1275-1282-1285-1287-1302-1307-1316"
type: feature
author: dominiklohmann
created: 2021-01-06T10:53:14Z
pr: 1208
---

VAST features a new plugin framework to support efficient customization points
at various places of the data processing pipeline. There exist several base
classes that define an interface, e.g., for adding new commands or spawning a
new actor that processes the incoming stream of data. The directory
`examples/plugins/example` contains an example plugin.
