---
title: "Add Python module for submitting queries to VAST"
type: bugfix
authors: tobim
pr: 685
---

A bug in the quoted string parser caused a parsing failure if an escape
character occurred in the last position.
