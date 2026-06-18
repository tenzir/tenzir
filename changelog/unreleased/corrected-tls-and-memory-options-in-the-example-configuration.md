---
title: Corrected mismatched defaults and options in the example configuration
type: bugfix
authors:
  - Zedoraps
  - claude
prs:
  - 6365
created: 2026-06-18T06:58:45.411704Z
---

The shipped example configuration now matches the options the node reads. The
setting that requires clients to present a certificate is
`tenzir.tls.require-client-cert`; the previously listed `tls-require-client-cert`
had no effect. The example also documents `tenzir.tls.password`, which decrypts
an encrypted `keyfile`, and no longer lists `tenzir.malloc-trim-interval`: the
node reads the trim interval only from the `TENZIR_ALLOC_TRIM_INTERVAL`
environment variable, never from the configuration file. The `plugins.platform`
TLS block no longer lists the server-side client-certificate options, because
the connection to the Tenzir Platform is an outbound client that does not use
them.

Two documented defaults were also wrong: `tenzir.retention.metrics` defaults to
`16d` (not `7d`), and `tenzir.start.disk-budget-check-interval` defaults to `60`
seconds (not `90`).

The `tenzir-node --help` output also reported the wrong default for
`rebuild-interval`. It now shows the actual default of `30min` instead of `2h`.
