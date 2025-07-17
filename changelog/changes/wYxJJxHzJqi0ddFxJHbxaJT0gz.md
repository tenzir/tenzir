---
title: "Add native Sigma support"
type: feature
authors: mavam
pr: 1379
---

[Sigma](https://github.com/Neo23x0/sigma) rules are now a valid format to
represent query expression. VAST parses the `detection` attribute of a rule and
translates it into a native query expression. To run a query using a Sigma rule,
pass it on standard input, e.g., `vast export json < rule.yaml`.
