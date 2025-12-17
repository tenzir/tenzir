---
title: "Allow the '-' in the expression key parser"
type: bugfix
author: tobim
created: 2020-08-04T13:57:02Z
pr: 999
---

A bug in the expression parser prevented the correct parsing of fields starting
with either 'F' or 'T'.
