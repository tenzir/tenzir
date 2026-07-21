---
title: Web identity token retrieval through proxies
type: bugfix
authors:
  - tobim
  - codex
prs:
  - 6458
created: 2026-07-20T14:42:53.876785Z
---

Nodes that use an outbound proxy now bypass it for IPv4 and IPv6 link-local
metadata endpoints. This restores `aws_iam.web_identity.token_endpoint`
authentication when the token is served from local instance metadata.
