---
title: "Explicitly close ending TCP connection sockets"
type: bugfix
author: tobim
created: 2024-10-28T09:56:16Z
pr: 4674
---

We fixed a bug that sometimes prevented incoming connections from `load_tcp`
from closing properly.
