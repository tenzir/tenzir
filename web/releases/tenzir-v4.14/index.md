---
title: Tenzir v4.14
authors: [IyeOnline]
date: 2024-05-17
tags: [release, slice, summarize, streaming-aggregation]
comments: true
---

Introducing [Tenzir
v4.14](https://github.com/tenzir/tenzir/releases/tag/v4.14.0): A major update to
the `summarize` operator with new aggreagtion functions, and support for slicing
with strides.

![Tenzir v4.14](tenzir-v4.14.excalidraw.svg)

<!-- truncate -->

## Introducing Streaming Aggregation

Our `summarize` operator just got a whole lot smarter! With new `timeout` and
`update-timeout` options, you can now perform streaming aggregations. These
settings determine how long a bucket remains active based on when the first and
last events arrive. `timeout` keeps events for a specified duration, while
`update-timeout` helps finalize buckets sooner when grouped events arrive
quickly.

## New Statistical Aggregation Functions

Get ready to enhance your data analysis with four new aggregation functions:

- `mean`: Calculates the average of grouped numeric values.
- `approximate_median`: Uses the t-digest algorithm to find an approximate
  median for grouped numbers.
- `stddev`: Computes the standard deviation of grouped numeric values.
- `variance`: Calculates the variance within the grouped data.

These functions will help you gain more insights and precision in your data
summaries.

## Enhanced Slicing with Strides

The `slice` operator now has a more flexible argument format: `<begin>:<end>`.
Here are some examples:

- `slice 10:`: Skips the first ten events.
- `slice 10:20`: Includes events from 10 to 19.
- `slice :-10`: Omits the last ten events.

We've also added support for strides. Use `slice <begin>:<end>:<stride>` to
specify steps between events. Want to reverse the event order? The new `reverse`
operator does just that, equivalent to `slice ::-1`.

## More Updates and Improvements

For detailed information on all the enhancements, adjustments, and fixes in this
release, check out our [changelog](/changelog#v4140).

Dive into the new features at [app.tenzir.com](https://app.tenzir.com), and be
sure to join the conversation on [our Discord server](/discord).

We hope you enjoy the enhancements in Tenzir v4.14!
