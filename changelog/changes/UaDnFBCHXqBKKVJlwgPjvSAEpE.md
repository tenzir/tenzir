---
title: "The index shall not quit on write errors"
type: bugfix
authors: tobim
pr: 2376
---

VAST will no longer terminate when it can't write any more data to disk.
Incoming data will still be accepted but discarded. We encourage all users to
enable the disk-monitor or compaction features as a proper solution to this
problem.
