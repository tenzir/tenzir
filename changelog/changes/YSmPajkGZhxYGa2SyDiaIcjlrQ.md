---
title: "Make unix dgram metrics sink connectionless"
type: bugfix
authors: tobim
pr: 1702
---

The UDS metrics sink continues to send data when the receiving socket is
recreated.
