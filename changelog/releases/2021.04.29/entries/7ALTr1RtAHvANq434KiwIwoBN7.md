---
title: "Make the source a regular class"
type: change
author: tobim
created: 2021-04-06T11:35:26Z
pr: 1498
---

The metrics for Suricata Eve JSON and Zeek Streaming JSON imports are now under
the categories `suricata-reader` and `zeek-reader` respectively so they can be
distinguished from the regular JSON import, which is still under `json-reader`.
