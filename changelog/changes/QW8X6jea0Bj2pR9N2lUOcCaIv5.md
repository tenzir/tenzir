---
title: "Make port-encoding for Arrow host-independent"
type: bugfix
authors: dominiklohmann
pr: 1007
---

The port encoding for Arrow-encoded table slices is now host-independent and
 always uses network-byte order.
