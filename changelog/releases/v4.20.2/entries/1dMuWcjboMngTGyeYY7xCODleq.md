---
title: "Allow binding to low ports in systemd"
type: bugfix
author: tobim
created: 2024-09-06T14:08:59Z
pr: 4580
---

The systemd unit now allows binding to privileged ports by default via the
ambient capability `CAP_NET_BIND_SERVICE`.
