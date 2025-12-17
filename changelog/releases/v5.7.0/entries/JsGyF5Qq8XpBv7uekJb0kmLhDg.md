---
title: "The `secret` function returns secrets"
type: change
author: IyeOnline
created: 2025-12-17T09:23:13Z
pr: [5065,5197]
---

The `secret` function now returns a `secret`, the strong type introduced in this
release. Previously it returned a plaintext `string`. This change protects
secrets from being leaked, as only operators can resolve secrets now.

If you want to retain the old behavior , you can enable the configuration option
`tenzir.legacy-secret-model`. In this mode, the `secret` function can only
resolve secrets from the Tenzir Node's configuration file and not access any
external secret store.
