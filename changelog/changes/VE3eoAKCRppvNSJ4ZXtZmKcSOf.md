---
title: "Schedule idle operators less aggressively"
type: bugfix
authors: dominiklohmann
pr: 3865
---

Some idle source operators and loaders, e.g., `from tcp://localhost:3000` where
no data arrives via TCP, consumed excessive amounts of CPU. This no longer
happens.
