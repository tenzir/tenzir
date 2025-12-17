---
title: "Make `read json --arrays-of-objects` faster"
type: bugfix
author: dominiklohmann
created: 2024-09-16T22:20:59Z
pr: 4601
---

We fixed an accidentally quadratic scaling with the number of top-level array
elements in `read json --arrays-of-objects`. As a result, using this option will
now be much faster.
