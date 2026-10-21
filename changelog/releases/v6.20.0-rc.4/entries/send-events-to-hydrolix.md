---
title: Send events to Hydrolix
type: feature
authors:
  - mavam
created: 2026-10-08T18:38:27.020536Z
---

The new [`to_hydrolix`](https://tenzir.com/docs/reference/operators/to_hydrolix) operator sends events to [Hydrolix](https://hydrolix.io) through its HTTP streaming ingest API:

```tql
subscribe "ocsf"
to_hydrolix "https://acme.hydrolix.live",
  token=secret("hydrolix-token"),
  table=f"security.ocsf_{class_uid}",
  transform="tenzir_ocsf"
```

The operator batches events per table and transform, so that a single pipeline can route events to multiple tables. It compresses requests with gzip and follows the Hydrolix integration guide: it retries rate-limited and failed requests with exponential backoff for at least five minutes while holding back new events, warns about events that Hydrolix rejects, and checks on startup whether the bearer token has expired or expires soon.
