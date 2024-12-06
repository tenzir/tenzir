---
title: "Tenzir Platform v1.5: Revamped Dashboards"
slug: tenzir-platform-v1.5
authors: [dit7ya, gitryder]
date: 2024-12-04
tags: [release, platform]
comments: true
---

Today we're announcing [Tenzir Platform v1.5][github-release], which brings a
richer dashboarding experience and adds a new contexts page.

![Tenzir Platform v1.5](tenzir-platform-v1.5.excalidraw.svg)

[github-release]: https://github.com/tenzir/platform/releases/tag/v1.5.0

## Multi-Node Dashboards

Dashboards received a major upgrade in this release.

In the past, every Tenzir Node had exactly one dashboard associated with it.
Now, dashboards are independent of nodes: Select your dashboard, and then select
what node to view it for.

Under the hood, we preserve which node you last viewed a dashboard for, and even
allow you to configure which node to view each individual chart or table on a
dashboard for. This makes building system-wide overviews, e.g., a health monitoring dashboard across nodes, finally possible.

To create a new dashboard, simply hit the "Add Dashboard" on the Dashboards
page, or create one on the fly when adding a chart to a dashboard in the
Explorer.

And this is just the start—we’ve got more dashboard improvements in the
pipeline, so stay tuned!

## Contexts Page

The new Contexts page allows for managing contexts directly in the Tenzir
Platform. Previously, creating a context required installing a package or
running a pipeline. Now, a visual interface guides you through the process.

The new interface makes context management faster, easier, and more intuitive,
fitting seamlessly into your daily workflow. Give it a try today!

## Other Improvements

As usual, we've squashed a lot of bugs on the way:

- Fixed an issue that prevented package uninstallation.
- Resolved a bug that could cause infinite loading when logging into a new
  account after logging out.
- Corrected an error that broke account deletion in the app.
- Fixed a bug that prevented package uninstallation.
- Fixed another bug of the app going into infinite loading in case of logging
  into a new account after logging out in the browser.

## Join Us for Office Hours

We'd love to connect with you at our office hours, held every second Tuesday at
5 PM CET on our [Discord server][discord]. Drop by for a chat—we always enjoy
hearing your thoughts and feedback!

[discord]: /discord
