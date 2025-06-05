---
title: "Make the source a regular class"
type: change
authors: tobim
pr: 1498
---

The metrics for Suricata Eve JSON and Zeek Streaming JSON imports are now under
the categories `suricata-reader` and `zeek-reader` respectively so they can be
distinguished from the regular JSON import, which is still under `json-reader`.
