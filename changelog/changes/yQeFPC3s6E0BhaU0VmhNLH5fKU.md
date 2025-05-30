---
title: "Explicitly close ending TCP connection sockets"
type: bugfix
authors: tobim
pr: 4674
---

We fixed a bug that sometimes prevented incoming connections from `load_tcp`
from closing properly.
