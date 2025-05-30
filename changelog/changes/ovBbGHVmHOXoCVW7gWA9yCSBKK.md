---
title: "Make `read json --arrays-of-objects` faster"
type: bugfix
authors: dominiklohmann
pr: 4601
---

We fixed an accidentally quadratic scaling with the number of top-level array
elements in `read json --arrays-of-objects`. As a result, using this option will
now be much faster.
