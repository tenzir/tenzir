---
title: "Fix logger deadlock in python tests"
type: bugfix
author: lava
created: 2024-02-07T09:14:15Z
pr: 3911
---

We fixed a rare deadlock by changing the internal logger behavior from blocking
until the oldest messages were consumed to overwriting them.
