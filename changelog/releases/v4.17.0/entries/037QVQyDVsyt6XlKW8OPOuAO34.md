---
title: "Remove built-in type aliases"
type: change
author: dominiklohmann
created: 2024-06-17T18:02:15Z
pr: 4299
---

The built-in type aliases `timestamp` and `port` for `time` and `uint64`,
respectively, no longer exist. They were an artifact of Tenzir from before it
supported schema inference in most parsers, and did not play well with many
operators when used together with inferred types from other parsers.
