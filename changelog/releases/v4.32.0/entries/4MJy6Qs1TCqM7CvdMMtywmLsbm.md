---
title: "Implement `to_google_secops`"
type: feature
author: raxyte
created: 2025-04-04T10:40:01Z
pr: 5101
---

We now provide an integration for customers with a Google SecOps workspace via
the `to_google_secops` operator. This new operator can send logs via the
[Chronicle Ingestion
API](https://cloud.google.com/chronicle/docs/reference/ingestion-api#unstructuredlogentries).
