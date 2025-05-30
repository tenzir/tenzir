---
title: "Fix the timezone shift to UTC for ISO8601 dates"
type: bugfix
authors: tobim
pr: 1537
---

A bug in the parsing of ISO8601 formatted dates that incorrectly adjusted the
time to the UTC timezone has been fixed.
