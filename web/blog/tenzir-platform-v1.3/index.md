---
title: Tenzir Platform v1.3
authors: [lava]
date: 2024-11-07
tags: [release, platform]
comments: true
---

[Tenzir Platform v1.3][github-release] brings a redesigned explorer page,
better behavior of the event inspector, and many other fixes.

![Tenzir Platform v1.3](./tenzir-platform-v1.3.excalidraw.svg)

[github-release]: https://github.com/tenzir/platform/releases/tag/v1.3.0

<!-- truncate -->

## Vertical Layout

In the explorer page, you can now find a new button to switch between
a horizontal and vertical layout.

![Vertical Layout](./tenzir-platform-v1.3.png)

The new vertical layout was designed to maximize the amount of screen
space available for the most important part of the explorer - the data!

Another goal was to make it easier to write long and complex pipelines
by increasing the number of lines that can be displayed on screen
at the same time.

## Other Fixes

We’ve implemented several other noteworthy fixes and enhancements:

 * The explorer event inspector now automatically selects the first event if it is open.
 * We fixed an issue in the detailed activity charts in the pipelines page where the ingress and egress activities were mistakenly swapped.
 * Charts in the dashboard show up to 10,000 events now.

## Join Us for Office Hours

Every second Tuesday at 5 PM CET, we hold our office hours on our [Discord
server][discord]. We love hearing your feedback—come join us for a chat!

[discord]: /discord
