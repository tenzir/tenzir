---
title: "Make amqp saver/loader detached"
type: bugfix
authors: IyeOnline
pr: 5206
---

We fixed a bug in `save_amqp` that caused the operator to not send any messages.
