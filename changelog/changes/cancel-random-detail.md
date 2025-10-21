---
title: "Memory usage when importing"
type: change
authors: jachris
pr: 5532
---

We optimized the memory usage of buffered synopses, which are used internally
when building indexes while importing events. The optimization significantly
reduces memory consumption by not performing copies of strings and IPs, roughly
cutting the memory usage of the underlying component in half.
