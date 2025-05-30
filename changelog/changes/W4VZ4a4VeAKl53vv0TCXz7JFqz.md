---
title: "PRs 4212-satta"
type: bugfix
authors: satta
pr: 4212
---

The `amqp` connector now properly signals more errors caused, for example, by
networking issues. This enables pipelines using this connector to trigger their
retry behavior.
