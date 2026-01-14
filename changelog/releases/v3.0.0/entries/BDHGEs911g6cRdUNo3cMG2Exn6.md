---
title: "Add a workaround to fix CAF OpenSSL options"
type: bugfix
author: tobim
created: 2023-02-02T18:31:11Z
pr: 2908
---

Options passed in the `caf.openssl` section in the configuration file or as
`VAST_CAF__OPENSSL__*` environment variables are no longer ignored.
