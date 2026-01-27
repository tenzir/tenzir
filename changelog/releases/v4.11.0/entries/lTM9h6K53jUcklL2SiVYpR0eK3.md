---
title: "Introduce the `every` operator modifier"
type: feature
author: dominiklohmann
created: 2024-03-19T13:23:48Z
pr: 4050
---

The `every <interval>` operator modifier executes a source operator repeatedly.
For example, `every 1h from http://foo.com/bar` polls an endpoint every hour.
