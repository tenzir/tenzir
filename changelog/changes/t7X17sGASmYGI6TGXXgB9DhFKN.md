---
title: "Fix logger deadlock in python tests"
type: bugfix
authors: lava
pr: 3911
---

We fixed a rare deadlock by changing the internal logger behavior from blocking
until the oldest messages were consumed to overwriting them.
