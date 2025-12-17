---
title: "Implement a retry mechanism for VAST clients failing to connect to the server"
type: feature
author: patszt
created: 2023-01-23T17:11:40Z
pr: 2835
---

We changed VAST client processes to attempt connecting to a VAST server multiple
times until the configured connection timeout (`vast.connection-timeout`,
defaults to 5 minutes) runs out. A fixed delay between connection attempts
(`vast.connection-retry-delay`, defaults to 3 seconds) ensures that clients to
not stress the server too much. Set the connection timeout to zero to let VAST
client attempt connecting indefinitely, and the delay to zero to disable the
retry mechanism.
