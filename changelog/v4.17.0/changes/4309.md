`show pipelines` now includes "hidden" pipelines run by the by the Tenzir
Platform or through the API. These pipelines usually run background jobs, so
they're intentionally hidden from the `/pipeline/list` API.
