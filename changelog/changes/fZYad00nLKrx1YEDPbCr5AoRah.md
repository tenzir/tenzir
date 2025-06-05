---
title: "Fix restart on failure"
type: bugfix
authors: dominiklohmann
pr: 3947
---

The option to automatically restart on failure did not correctly trigger for
pipelines that failed an operator emitted an error diagnostic, a new mechanism
for improved error messages introduced with Tenzir v4.8. Such pipelines now
restart automatically as expected.
