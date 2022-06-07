VAST now reads the default false-positive rate for its sketches correctly. This
was accndeitally broken with the v2.0 release. The option moved from
`vast.catalog-fp-rate` to `vast.index.default-fp-rate`.
