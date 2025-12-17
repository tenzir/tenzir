---
title: "Disable 0mq socket lingering"
type: change
author: mavam
created: 2023-10-02T13:10:24Z
pr: 3536
---

We made it easier to reuse the default `zmq` socket endpoint by disabling
*socket lingering*, and thereby immediately relinquishing resources when
terminating a ZeroMQ pipeline. Changing the linger period from infinite to 0 no
longer buffers pending messages in memory after closing a ZeroMQ socket.
