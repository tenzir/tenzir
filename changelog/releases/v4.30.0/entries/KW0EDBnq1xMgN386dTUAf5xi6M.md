---
title: "Add a global ca-certificates config option"
type: feature
author: tobim
created: 2025-03-13T09:49:49Z
pr: 5022
---

We introduced common TLS settings for all operators that support TLS. The Tenzir
config now has a key `cacert`, which will set the CA certificate file for all
operators using it. The default for this will be chosen appropriately for the
system.
