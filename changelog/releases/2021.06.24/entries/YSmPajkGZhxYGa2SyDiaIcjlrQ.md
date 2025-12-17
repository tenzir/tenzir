---
title: "Make unix dgram metrics sink connectionless"
type: bugfix
author: tobim
created: 2021-06-08T09:01:44Z
pr: 1702
---

The UDS metrics sink continues to send data when the receiving socket is
recreated.
