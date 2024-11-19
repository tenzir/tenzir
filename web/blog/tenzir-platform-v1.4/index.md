---
title: "Tenzir Platform v1.4: Platform Alerts"
authors: [lava]
date: 2024-11-07
tags: [release, platform]
comments: true
---

[Tenzir Platform v1.4][github-release] adds platform alerts and
many fixes and improvements to the frontend.

![Tenzir Platform v1.4](./tenzir-platform-v1.4.excalidraw.svg)

[github-release]: https://github.com/tenzir/platform/releases/tag/v1.4.0

<!-- truncate -->

## Platform Alerts

The platform now supports monitoring individual nodes and triggering an
alert when the node stays disconnected for more than a configurable
threshold

This is useful for production nodes that are supposed to be online 24/7.
With alerts, it becomes easier

See [Platform CLI](/platform-cli) docs page for an overview and a usage
example.

## Other Improvements

As usual there's also a slew of additional improvements and bug fixes
provided in this release:

 * The tenant switcher is now significantly faster
 * Added a new diagnostics drawer in dashboard page
 * Fixed image downloads for charts

## Join Us for Office Hours

Every second Tuesday at 5 PM CET, we hold our office hours on our [Discord
server][discord]. We love hearing your feedback—come join us for a chat!

[discord]: /discord
