---
title: Graceful shutdown for save_tcp connector
type: bugfix
author: raxyte
pr: 5637
created: 2025-12-29T10:35:19.784178Z
---

The `save_tcp` connector now gracefully shuts down on pipeline stop and
connection failures. Previously, the connector could abort the entire
application on exit.
