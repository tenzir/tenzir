---
title: Local PII and entity detection with the ai::entities operator
type: feature
authors:
  - zedoraps
  - claude
prs:
  - 6429
created: 2026-07-07T19:13:58.106797Z
---

The new `ai::entities` operator detects named entities—such as PII—in string
fields using local GLiNER models, without any data leaving the node.

```tql
from {message: "Alice logged in from 203.0.113.4 using alice@example.com"}
ai::entities field=message,
             model="/var/lib/tenzir/models/gliner-pii",
             labels=["name", "email address", "ip address"],
             into=pii
```

The operator writes a list of `{text, label, start, end, score}` records with
UTF-8 byte offsets into the field given by `into` (default: `ai.entities`).
Labels are zero-shot: pass any entity types as plain strings, and tune
sensitivity with `threshold` (default: `0.5`).

Point `model` at a directory containing a span-level GLiNER ONNX model. We
recommend
[knowledgator/gliner-pii-base-v1.0](https://huggingface.co/knowledgator/gliner-pii-base-v1.0)
(Apache 2.0), which detects 60+ PII/PHI types and works best with its trained
label names, such as `name`, `email address`, `ip address`, `username`,
`password`, and `credit card`. Inference runs in-process on the CPU—ideal for
detecting and redacting sensitive data in regulated environments.
