---
title: "Introduce the `every` operator modifier"
type: feature
authors: dominiklohmann
pr: 4050
---

The `every <interval>` operator modifier executes a source operator repeatedly.
For example, `every 1h from http://foo.com/bar` polls an endpoint every hour.
