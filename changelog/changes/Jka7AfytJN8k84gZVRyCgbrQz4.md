---
title: "Enhance HTTP connector controls"
type: feature
authors: mavam
pr: 4811
---

Several new options are now available for the `load_http` operator: `data`,
`json`, `form`, `skip_peer_verification`, `skip_hostname_verification`,
`chunked`, and `multipart`. The `skip_peer_verification` and
`skip_hostname_verification` options are now also available for the `save_http`
operator.
