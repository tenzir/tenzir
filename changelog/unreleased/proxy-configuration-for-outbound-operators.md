---
title: Configure an HTTP proxy for outbound operators
type: feature
authors:
  - raxyte
created: 2026-05-25T00:00:00.000000Z
---

Tenzir nodes can now route outbound traffic from operators through an
HTTP/HTTPS proxy. Set `tenzir.http-proxy` for `http://` targets and
`tenzir.https-proxy` for `https://` and gRPC targets in `tenzir.yaml` (or via
the matching `TENZIR_HTTP_PROXY` / `TENZIR_HTTPS_PROXY` environment variables)
to URLs such as `http://proxy.example.com:3128`; Basic-auth userinfo is
supported.

`tenzir.no-proxy` accepts the same suffix-list syntax as the `NO_PROXY`
environment variable and lets specific destinations bypass the proxy. CIDR
entries match IP-literal destinations. `TENZIR_NO_PROXY` provides the matching
environment override. Loopback addresses (`localhost`, `127.0.0.0/8`, `::1`)
always bypass.

The setting applies to operators that perform outbound HTTP, HTTPS,
or gRPC requests — `from_s3` / `to_s3`, `from_google_cloud_storage` /
`to_google_cloud_storage`, `from_azure_blob_storage` / `to_abs`,
`from_http` / `to_http`, `to_elasticsearch`, `from_sqs` / `to_sqs`,
`from_amazon_cloudwatch` / `to_amazon_cloudwatch`,
`from_velociraptor`, `from_google_cloud_pubsub` /
`to_google_cloud_pubsub`, `to_google_cloud_logging`, the
libcurl-backed `from "https://…"` / `to "https://…"` connectors, and
the Tenzir Platform WebSocket.

For gRPC-backed operators, gRPC Core accepts only `http://` proxy URLs; it
rejects `https://` proxy URLs in its own proxy mapper.

The proxy URL must include an explicit port, since the default-port
behaviour of the underlying SDKs is not portable.

Caveat: the GCS default credential chain first probes the metadata
server at `metadata.google.internal`, which is on google-cloud-cpp's
hard-coded `NO_PROXY` list. Workload-identity / metadata lookups will
not traverse the proxy; anonymous or explicitly-credentialed flows
do.

The proxy URL lives plaintext in YAML; promotion to a `secret` is
deferred.
