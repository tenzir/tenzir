---
title: "Change default endpoint to 127.0.0.1"
type: change
author: lava
created: 2022-09-01T17:09:32Z
pr: 2512
---

We changed the default VAST endpoint from `localhost` to `127.0.0.1`. This
ensures the listening address is deterministic and not dependent on the
host-specific IPv4 and IPv6 resolution. For example, resolving `localhost`
yields a list of addresses, and if VAST fails to bind on the first (e.g., to due
to a lingering socket) it would silently go to the next. Taking name resolution
out of the equation fixes such issues. Set the option `vast.endpoint` to
override the default endpoint.
