---
title: "Fix :timestamp queries for old data"
type: bugfix
authors: tobim
pr: 1432
---

Data that was ingested before the deprecation of the `#timestamp` attribute
wasn't exported correctly with newer versions. This is now corrected.
