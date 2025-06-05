---
title: "Remove the shutdown grace period"
type: bugfix
authors: dominiklohmann
pr: 2568
---

VAST no longer attempts to hard-kill itself if the shutdown did not finish
within the configured grace period. The option `vast.shutdown-grace-period` no
longer exists. We recommend setting `TimeoutStopSec=180` in the VAST systemd
service definition to restore the previous behavior.
