---
title: "Fix partial specialization of S3 configuration in URL"
type: bugfix
author: dominiklohmann
created: 2024-03-11T08:55:56Z
pr: 4001
---

The S3 connector no longer ignores the default credentials provider for the
current user when any arguments are specified in the URI explicitly.
