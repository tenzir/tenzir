---
title: "Require expressions to be parsed to end-of-input"
type: feature
author: dominiklohmann
created: 2020-03-10T15:54:44Z
pr: 791
---

Expressions must now be parsed to the end of input. This fixes a bug that caused
malformed queries to be evaluated until the parser failed. For example, the
query `#type == "suricata.http" && .dest_port == 80` was erroneously evaluated
as `#type == "suricata.http"` instead.
