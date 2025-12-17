---
title: "Implement `from_http`"
type: feature
author: raxyte
created: 2025-04-14T14:43:10Z
pr: 5114
---

`from_http <host:port>, server=true` creates an HTTP/1.1 server that listens on
a specified hostname and port. In the future, the `load_http` operator's HTTP
client will be integrated with this operator as well, eventually superseding
`load_http`.
