---
title: Google Cloud Logging sink in the static builds
type: bugfix
authors:
  - tobim
  - claude
created: 2026-08-22T00:00:00.000000Z
---

The `to_google_cloud_logging` operator is now part of the static builds, which
includes the `-slim` container images and the packages on
[get.tenzir.app](https://get.tenzir.app). Their `google-cloud-cpp` dependency
was built without the `logging` API, so the plugin disabled itself at configure
time with nothing but a warning to show for it.
