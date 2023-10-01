---
title: Tenzir v4.3
authors: [jachris]
date: 2023-10-10
tags: [release, pipelines, json, yaml]
draft: true
---

TODO: Write intro and add thumbnail image

![Tenzir v4.3](tenzir-v4.3.excalidraw.svg)

<!--truncate-->

## JSON Parser

We've revamped our JSON parser to be a lot faster and more accurate in type
inference.

TODO: Add benchmarks
TODO: Explain what changed about type inference

## YAML Format

Tenzir now supports reading and writing YAML documents and document streams. Use
`read yaml` and `write yaml` to parse and print YAML, respectively.

TODO: Show some examples, and explain why users would want to have this

## Data Model Tweaks

TODO: Write about empty records
TODO: Write about the null type, and how it enables empty lists
