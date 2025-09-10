---
title: "Simplify sending JSON events to Kafka"
type: change
authors: mavam
pr: 5466
---

The `to_kafka` operator now automatically formats events as JSON when you don't
specify the `message` argument, defaulting to `this.print_json()`. This
eliminates boilerplate from pipelines—just write `to_kafka "events"` instead of
`to_kafka "events", message=this.print_json()` to stream JSON-formatted data to
Kafka.
