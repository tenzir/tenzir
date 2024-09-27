The CEF, CSV, GELF, JSON, KV, LEEF, Suricata, Syslog, XSV, YAML and Zeek JSON
parsers now properly adhere to the schema of the read data. Previously, parsers
would merge heterogeneous input into a single, growing schema,
inserting nulls for fields that did not exist in some events.

The `fluent-bit` source now properly adheres to the schema of the read data.

The CEF, CSV, GELF, JSON, KV, LEEF, Suricata, Syslog, XSV, YAML and Zeek JSON
parsers now all support the `--schema`, `--selector` flags to parse their data
according to some given schema, as well as various other flags to more
precisely control their output schema.
