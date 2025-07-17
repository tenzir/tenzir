---
title: "PRs 1445-1452"
type: change
authors: dominiklohmann
pr: 1445
---

Plugins can now be linked statically against VAST. A new `VASTRegisterPlugin`
CMake function enables easy setup of the build scaffolding required for plugins.
Configure with `--with-static-plugins` or build a static binary to link all
plugins built alongside VAST statically. All plugin build scaffoldings must be
adapted, older plugins do no longer work.
