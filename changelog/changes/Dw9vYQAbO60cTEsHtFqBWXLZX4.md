---
title: "Fix datagram source actor not running heartbeat"
type: bugfix
authors: dominiklohmann
pr: 662
---

The import process did not print statistics when importing events over UDP.
Additionally, warnings about dropped UDP packets are no longer shown per packet,
but rather periodically reported in a readable format.
