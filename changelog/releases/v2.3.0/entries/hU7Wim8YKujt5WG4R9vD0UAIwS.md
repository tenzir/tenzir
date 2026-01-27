---
title: "Make the connection timeout configurable"
type: feature
author: dominiklohmann
created: 2022-08-16T10:47:28Z
pr: 2499
---

The new `vast.connection-timeout` option allows for configuring the timeout VAST
clients use when connecting to a VAST server. The value defaults to 10s; setting
it to a zero duration causes produces an infinite timeout.
