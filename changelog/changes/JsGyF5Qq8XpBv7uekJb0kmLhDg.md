---
title: "The `secret` function returns secrets"
type: change
authors: IyeOnline
pr: [5065,5197]
---

The `secret` function now returns a `secret`, the strong type introduced in this
release. Previously it returned a plaintext `string`.

If you wish to retain the previous behavior, you can enable the config option
`tenzir.legacy-secret-model`. This option is for will be removed in the future.
