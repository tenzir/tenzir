---
title: "Add new query language plugin"
type: feature
author: mavam
created: 2022-02-11T10:36:14Z
pr: 2074
---

VAST has a new *query language* plugin type that allows for adding additional
query language frontends. The plugin performs one function: compile user input
into a VAST expression. The new `sigma` plugin demonstrates usage of this plugin
type.
