---
title: "PRs 4212-satta"
type: bugfix
author: satta
created: 2024-05-12T10:49:48Z
pr: 4212
---

The `amqp` connector now properly signals more errors caused, for example, by
networking issues. This enables pipelines using this connector to trigger their
retry behavior.
