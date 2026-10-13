---
title: Write metrics to Google Cloud Monitoring
type: feature
authors:
  - mavam
created: 2026-10-13T10:46:13.918049Z
---

The new `to_google_cloud_monitoring` operator writes metrics directly to Google Cloud Monitoring, without an intermediate Prometheus Remote Write receiver or OpenTelemetry Collector. It accepts the same fields as `to_prometheus` and maps gauges and counters to gauge and cumulative metrics. Histograms in the OTLP layout, as produced by `accept_otlp`, become distribution metrics:

```tql
accept_otlp "0.0.0.0:4317", signals=["metrics"], schema="record"
where @name == "otel.metric.histogram"
where aggregation_temporality == "cumulative"
to_google_cloud_monitoring name=metric.name, help=metric.description,
  unit=metric.unit, timestamp=time, start_timestamp=start_time,
  labels=attributes, type="histogram"
```

The operator creates missing metric descriptors, identifies the writing node as a Compute Engine instance or as the monitored resource from `resource_type` and `resource_labels`, and keeps the newest point per series within each `flush_interval`.
