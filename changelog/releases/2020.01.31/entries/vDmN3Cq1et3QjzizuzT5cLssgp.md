---
title: "Add Python module for submitting queries to VAST"
type: bugfix
author: tobim
created: 2020-01-27T10:18:45Z
pr: 685
---

A bug in the quoted string parser caused a parsing failure if an escape
character occurred in the last position.
