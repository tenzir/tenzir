---
title: "Add Zeek Broker reader plugin"
type: feature
author: mavam
created: 2021-07-08T15:55:50Z
pr: 1758
---

The new [Broker](https://github.com/zeek/broker) plugin enables seamless log
ingestion from [Zeek](https://github.com/zeek/zeek) to VAST via a TCP socket.
Broker is Zeek's messaging library and the plugin turns VAST into a Zeek [logger
node](https://docs.zeek.org/en/master/frameworks/cluster.html#logger). Use
`vast import broker` to establish a connection to a Zeek node and acquire logs.
