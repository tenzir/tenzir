---
title: "Handling HTTP error status codes"
type: feature
authors: raxyte
pr: 5358
---

The `from_http` and `http` operators now provide an `error_field` option that
lets you specify a field to receive the error response as a `blob`. When you set
this option, the operators keep events with status codes outside the 200–399
range so you can handle them manually.
