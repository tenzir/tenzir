---
title: "Tenzir Platform v1.5: Multi-node Dashboards, Contexts, and more"
slug: tenzir-platform-v1.5
authors: [dit7ya, gitryder]
date: 2024-12-04
tags: [release, platform]
comments: true
---

We’re thrilled to share that [Tenzir Platform v1.5][github-release] brings you a richer dashboarding experience, smoother context management, and a range of bug fixes and small enhancements throughout the app.

![Tenzir Platform v1.5](tenzir-platform-v1.5.excalidraw.svg)

[github-release]: https://github.com/tenzir/platform/releases/tag/v1.5.0

## Multi-Node Dashboards

The dashboarding experience in the app has received a major upgrade.

In the past, each Tenzir node had its own dedicated dashboard, and they were always linked. Now, dashboards are a standalone feature—you can create, rename, and delete them as needed. They can also include charts or tables from multiple nodes, making it easy to build system-wide overviews, like a health monitoring dashboard for all your nodes.

You can adjust the execution node for a chart or table on the fly using the three-dot menu or apply changes in bulk with the node applier in the top bar.

For those paying close attention, the dashboards page route has been updated from `/dashboard` to `/dashboards`, with a redirect in place to ensure uninterrupted access.

And this is just the start—we’ve got more dashboard improvements in the pipeline, so stay tuned!

## Contexts Page

We’ve made managing your Tenzir contexts more straightforward with the addition of a dedicated Contexts page. Now you can see all your contexts in one convenient place, simplifying how you keep track of them.

Inspecting or deleting a context is as simple as a single click, while creating a new one is just as easy—hit the “Add Context” button, and you’re good to go, no commands necessary.

This update makes context management faster, easier, and more intuitive, fitting seamlessly into your daily workflow. Give it a try today!

## Other Improvements

We’ve also been busy refining the platform with a series of bug fixes and improvements:

- Fixed an issue that prevented package uninstallation.
- Resolved a bug that could cause infinite loading when logging into a new account after logging out.
- Corrected an error that broke account deletion in the app.

## Join Us for Office Hours

We’d love to connect with you at our office hours, held every second Tuesday at 5 PM CET on our [Discord server][discord]. Drop by for a chat—we always enjoy hearing your thoughts and feedback!

[discord]: /discord
