---
title: Sink byte metrics count the delivered payload
type: change
authors:
  - mavam
created: 2026-11-12T09:34:37.023516Z
---

Sink operators now report the bytes they write consistently: the payload before
any compression they apply, counted once it was delivered, and once however many
retries it took. Together with sources, which report the bytes they read, this
makes the bytes going into and out of a pipeline comparable.

For example, `to_splunk`, `to_hydrolix`, `to_opensearch`, `to_prometheus`,
`to_google_secops`, `to_azure_log_analytics`, and `to_sentinelone_data_lake` now
count the request body before compressing it, and only once the destination
accepted it. `to_google_secops` counts nothing in `dry_run` mode.
`to_google_cloud_logging` counts entries once Cloud Logging accepted them,
`to_http` with `buffer_all` counts the body once the request succeeded, and
`to_kafka` counts a message once the brokers acknowledged it.
