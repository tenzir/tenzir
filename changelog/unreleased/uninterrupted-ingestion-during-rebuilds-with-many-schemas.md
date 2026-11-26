---
title: Uninterrupted ingestion during rebuilds with many schemas
type: bugfix
authors:
  - tobim
created: 2026-11-26T15:30:00.000000Z
---

Ingestion no longer stalls for minutes while a rebuild runs on a node that stores many distinct schemas.

Previously, while a rebuild was in progress, the node spent over a second choosing its next batch after every newly written partition. On a node with hundreds of schema variants and around 100,000 partitions, this kept the partition bookkeeping fully busy, so incoming events piled up and sources stopped receiving data until the rebuild finished.
