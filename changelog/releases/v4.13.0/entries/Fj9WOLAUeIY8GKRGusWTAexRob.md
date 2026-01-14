---
title: "Fix CSV/XSV format printing the header once for each batch"
type: bugfix
author: jachris
created: 2024-05-08T12:55:20Z
pr: 4195
---

The CSV, TSV, and SSV printers no longer erroneously print the header multiple
times when more than one event batch of events arrives.
