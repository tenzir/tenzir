---
title: "Increase the partition erase timeout to 1 minute"
type: bugfix
authors: tobim
pr: 1897
---

The timeout duration to delete partitions has been increased to one minute,
reducing the frequency of warnings for hitting this timeout significantly.
