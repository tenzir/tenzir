The bundled Zeek schema no longer includes the `_path` field included in Zeek
JSON. Use `#type == "zeek.foo"` over `_path == "foo"` for querying data ingested
using `vast import zeek-json`.
