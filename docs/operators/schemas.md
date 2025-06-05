---
title: schemas
category: Node/Storage Engine
example: 'schemas'
---

Retrieves all schemas for events stored at a node.

```tql
schemas
```

## Description

The `schemas` operator shows all schemas of all events stored at a node.

Note that there may be multiple schema definitions with the same name, but a
different set of fields, e.g., because the imported data's schema changed over
time.

## Examples

### See all available definitions for a given schema

```tql
schemas
where name == "suricata.alert"
```
