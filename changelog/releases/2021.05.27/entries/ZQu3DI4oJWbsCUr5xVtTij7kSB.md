---
title: "Fix the datagram source"
type: bugfix
author: dominiklohmann
created: 2021-05-03T13:39:17Z
pr: 1622
---

A recent change caused imports over UDP not to forward its events to the VAST
server process. Running `vast import -l :<port>/udp <format>` now works as
expected again.
