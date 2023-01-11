The per-schema event distribution moved from `index.statistics.layouts` to
`catalog.schemas`, and additionally includes information about the import time
range and the number of partitions VAST knows for the schema. The number of
events per schema no longer includes events that are yet unpersisted.
