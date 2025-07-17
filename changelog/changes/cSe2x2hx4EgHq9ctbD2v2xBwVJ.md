---
title: "Allow read access to user home dir in the systemd unit"
type: bugfix
authors: tobim
pr: 2734
---

The systemd service no longer fails if the home directory of the vast user is
not in `/var/lib/vast`.
