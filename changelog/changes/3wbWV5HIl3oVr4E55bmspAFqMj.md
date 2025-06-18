---
title: "Add a global ca-certificates config option"
type: change
authors: tobim
pr: 5022
---

The `skip_host_verification` option has been removed from the `load_http`,
`save_email` and `save_http` operators. Its functionality has been merged into
the `skip_peer_verification` option.
