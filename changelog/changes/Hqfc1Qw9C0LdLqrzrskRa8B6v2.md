---
title: "Set fallback port for underspecified endpoints"
type: change
authors: dominiklohmann
pr: 1170
---

VAST now listens on port 42000 instead of letting the operating system choose
the port if the option `vast.endpoint` specifies an endpoint without a port. To
restore the old behavior, set the port to 0 explicitly.
