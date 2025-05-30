---
title: "Fix partial specialization of S3 configuration in URL"
type: bugfix
authors: dominiklohmann
pr: 4001
---

The S3 connector no longer ignores the default credentials provider for the
current user when any arguments are specified in the URI explicitly.
