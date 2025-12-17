---
title: "Fix :timestamp queries for old data"
type: bugfix
author: tobim
created: 2021-03-11T16:24:30Z
pr: 1432
---

Data that was ingested before the deprecation of the `#timestamp` attribute
wasn't exported correctly with newer versions. This is now corrected.
