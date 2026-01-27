---
title: "Implement a ZeroMQ connector"
type: feature
author: mavam
created: 2023-09-11T15:47:21Z
pr: 3497
---

The new `zmq` connector ships with a saver and loader for interacting with
ZeroMQ. The loader (source) implements a connecting `SUB` socket and the saver
(sink) a binding `PUB` socket. The `--bind` or `--connect` flags make it
possible to control the direction of connection establishment.
