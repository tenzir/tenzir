---
title: A disabled node endpoint fails node connections at once
type: bugfix
authors:
  - lava
created: 2026-11-11T16:08:28.312201Z
---

Setting `tenzir.endpoint` to `false` now also tells clients that there is no
node to connect to. Before, a pipeline that needed a node, for example to
resolve a secret, ignored the setting and kept retrying the default endpoint
`127.0.0.1:5158` until the connection timeout ran out, logging a warning every
few seconds. Now such a pipeline fails right away with an error that names the
disabled endpoint.
