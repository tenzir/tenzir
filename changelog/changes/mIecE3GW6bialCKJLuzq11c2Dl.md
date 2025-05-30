---
title: "Fix loading of the optional OpenSSL module"
type: bugfix
authors: dominiklohmann
pr: 1740
---

Configuring VAST to use CAF's built-in OpenSSL module via the `caf.openssl.*`
options now works again as expected.
