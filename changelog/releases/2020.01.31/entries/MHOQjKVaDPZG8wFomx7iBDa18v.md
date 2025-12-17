---
title: "Add support for Apache Arrow"
type: feature
author: dominiklohmann
created: 2019-12-02T11:01:39Z
pr: 633
---

Added *Apache Arrow* as new export format. This allows users to export query
results as Apache Arrow record batches for processing the results downstream,
e.g., in Python or Spark.
