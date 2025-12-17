---
title: "Let the JSON reader recover from unexpected inputs"
type: bugfix
author: tobim
created: 2021-02-17T15:45:43Z
pr: 1386
---

A bug in the new simdjson based JSON reader introduced in
[#1356](https://github.com/tenzir/vast/pull/1356) could trigger an assertion in
the `vast import` process if an input field could not be converted to the field
type in the target layout. This is no longer the case.
