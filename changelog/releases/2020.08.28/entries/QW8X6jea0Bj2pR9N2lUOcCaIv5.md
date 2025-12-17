---
title: "Make port-encoding for Arrow host-independent"
type: bugfix
author: dominiklohmann
created: 2020-08-06T17:55:30Z
pr: 1007
---

The port encoding for Arrow-encoded table slices is now host-independent and
 always uses network-byte order.
