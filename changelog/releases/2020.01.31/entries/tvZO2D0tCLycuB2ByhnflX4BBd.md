---
title: "Register the accountant for datagram sources"
type: bugfix
author: dominiklohmann
created: 2019-11-19T12:07:43Z
pr: 655
---

Importing events over UDP with `vast import <format> --listen :<port>/udp`
failed to register the accountant component. This caused an unexpected message
warning to be printed on startup and resulted in losing import statistics. VAST
now correctly registers the accountant.
