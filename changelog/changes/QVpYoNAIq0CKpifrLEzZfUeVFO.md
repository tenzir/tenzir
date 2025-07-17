---
title: "Fix `package::add`"
type: bugfix
authors: raxyte
pr: 5271
---

The `package::add` operator did not correctly handle the switch to `from_http`
in the previous release and as a result errored when installing packages
manually. This has now been fixed. However, package installation via Tenzir
Platform was still functional. This was also the cause of the demo node not
having any pipelines or pre-installed packages when launched.
