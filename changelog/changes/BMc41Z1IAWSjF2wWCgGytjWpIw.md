---
title: "Allow the '-' in the expression key parser"
type: bugfix
authors: tobim
pr: 999
---

A bug in the expression parser prevented the correct parsing of fields starting
with either 'F' or 'T'.
