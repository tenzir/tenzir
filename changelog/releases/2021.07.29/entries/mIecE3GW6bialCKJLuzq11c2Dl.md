---
title: "Fix loading of the optional OpenSSL module"
type: bugfix
author: dominiklohmann
created: 2021-07-08T13:39:04Z
pr: 1740
---

Configuring VAST to use CAF's built-in OpenSSL module via the `caf.openssl.*`
options now works again as expected.
