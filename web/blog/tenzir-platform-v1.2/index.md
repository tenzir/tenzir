---
title: Tenzir Platform v1.2
authors: [dominiklohmann]
date: 2024-10-23
tags: [release, platform]
comments: true
---

[Tenzir Platform v1.2][github-release] brings improvements to diagnostics in the
Explorer, the ability to download charts, and many stability improvements fixes.

![Tenzir Platform v1.2](tenzir-platform-v1.2.excalidraw.svg)

[github-release]: https://github.com/tenzir/platform/releases/tag/v1.2.0

<!-- truncate -->

## Simplified Diagnostics Flow

In our previous version, the diagnostics pane would automatically open when a
pipeline encountered an error or completed without results but with at least one
warning. While this feature aimed to provide immediate feedback, it often led to
confusion as the pane never closed automatically. We recognized that
automatically closing it could disrupt your workflow by causing layout shifts
while iterating on your pipelines.

With the new update, we’ve simplified the user experience: the diagnostics pane
will no longer open or close automatically. Instead, you can easily access it by
clicking on the diagnostics count or the newly added toasts for errors. This
change ensures a smoother and more intuitive interaction with the diagnostics
feature.

## Downloading Charts

We’ve also made it easier for you to manage your visual data. You can now save
charts in the Explorer or on Dashboards in either SVG or PNG formats.
Additionally, we’ve updated the chart colors to improve contrast, making it
easier to distinguish between different data points.

## Other Fixes

We’ve implemented several other noteworthy fixes and enhancements:

- The pesky 408 Proxy Timeout errors in the Explorer for pipelines that run for
  a longer period of time no longer exist.
- We fixed a bug that caused the detailed pipeline activity to render
  incorrectly. Note that rendering the activity chart now requires running at
  least Tenzir Node v4.22.
- For Sovereign Edition users, the env variable `PUBLIC_OIDC_SCOPES` allows for
  overriding the default OIDC scopes.

## Join Us for Office Hours

Every second Tuesday at 5 PM CET, we hold our office hours on our [Discord
server][discord]. We love hearing your feedback—come join us for a chat!

[discord]: /discord
