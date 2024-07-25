---
sidebar_custom_props:
  format:
    parser: true
---

# zeek-json

The `zeek-json` format is an alias for [`json`](json.md) with the arguments:

- `--selector=_path:zeek`
- `--unnest-separator="."`
- `--ndjson`

All other options from [`json`](json.md) are also supported, including the common [schema inference options](formats.md#parser-schema-inference).
