---
title: "Support 0mq inproc sockets"
type: feature
authors: mavam
pr: 4117
---

The `0mq` connector now supports `inproc` socket endpoint URLs, allowing you to
create arbitrary publish/subscribe topologies within a node. For example, `save
zmq inproc://foo` writes messages to the in-process socket named `foo`.
