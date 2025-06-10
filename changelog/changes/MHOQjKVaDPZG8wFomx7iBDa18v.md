---
title: "Add support for Apache Arrow"
type: feature
authors: dominiklohmann
pr: 633
---

Added *Apache Arrow* as new export format. This allows users to export query
results as Apache Arrow record batches for processing the results downstream,
e.g., in Python or Spark.
