---
title: "Add a workaround to fix CAF OpenSSL options"
type: bugfix
authors: tobim
pr: 2908
---

Options passed in the `caf.openssl` section in the configuration file or as
`VAST_CAF__OPENSSL__*` environment variables are no longer ignored.
