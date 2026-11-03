---
title: Ordered input for taste and throttle with drop
type: bugfix
authors:
  - mavam
created: 2026-11-02T19:48:29.000000Z
---

`taste` and `throttle drop=true` keep the first events they see, but the optimizer could reorder their input, for example by removing a preceding `sort`. They now keep the first events in the original order, so `sort x | taste 1` returns the event with the smallest `x`.
