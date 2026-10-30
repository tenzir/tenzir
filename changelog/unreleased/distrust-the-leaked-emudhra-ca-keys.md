---
title: Distrust the leaked eMudhra CA keys
type: bugfix
authors:
  - lava
created: 2026-10-29T13:58:51.711945Z
---

The container image no longer trusts certificates issued by eMudhra intermediate CAs whose private keys leaked. It now ships the NSS 3.131.1 root store, and the root stores bundled with gRPC and LibreSSL point at that same store.
