---
title: "Tenzir Platform v1.7: Explorer Drag'n'Drop"
slug: tenzir-platform-v1.7
authors: [lava]
date: 2025-01-08
tags: [release, platform]
comments: true
---

To kick off the new year, we're releasing [Tenzir Platform v1.7][github-release], featuring support for file drag and drop and a lot
of stability improvements..

![Tenzir Platform v1.7](tenzir-platform-v1.7.png)

[github-release]: https://github.com/tenzir/platform/releases/tag/v1.7.0

<!-- truncate -->

## Explorer Drag'n'Drop

It is now possible to drag and drop files to the explorer window in order to create a pipeline reading the data from the dropped file.

<!-- todo: image -->

## Webhook Analytics

For sovereign edition users, it is now possible to specify a webhook URL
as a destination for analytics events.

To configure this URL, set the `ANALYTICS_WEBHOOK_URL` environment variable
in the platform environment to the desired value. If no URL is configured,
no analytics are collected.

Note that analytics events are currently sent synchronously, so a slow
analytics sink has the potential to slow down regular platform operations.

## Stability Improvements

Our recent focus on app stability continues to yield steady
improvements that improve the quality of life for all users of the
Tenzir Platform.

For example, we now avoid unnecessary network activity by preventing
the pipeline list requests from targeting offline nodes, we fixed the
pipeline list spinner showing unnecessarily when changing a pipeline
and we resolved an issue with pipeline keepalive daemons persisting longer than needed.

## Bug fixes

This release also contains several additional bugfixes:

- Fix sparkbar metrics query crashing when selecting offline nodes.
- Fix the keyboard shortcut triggering a pipeline rerun instead of confirming the modal.
- Fix dashboard creation with an empty name.
- Ensure "Add to dashboard" adds content based on the currently selected table or chart view.
- Fix several issues related to detailed activity metrics.

## Join Us for Office Hours

Join us for our bi-weekly office hours every other Tuesday at 5 PM CET on our
[Discord server][discord]. It's a great opportunity to share your experiences,
ask questions, and help shape the future of Tenzir with your valuable feedback!

[discord]: /discord
