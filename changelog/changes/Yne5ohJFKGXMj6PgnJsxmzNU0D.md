---
title: "Add a `velociraptor` operator"
type: feature
authors: mavam
pr: 3556
---

The new `velociraptor` source supports submitting VQL queries to a Velociraptor
server. The operator communicates with the server via gRPC using a mutually
authenticated and encrypted connection with client certificates. For example,
`velociraptor -q "select * from pslist()"` lists processes and their running
binaries.
