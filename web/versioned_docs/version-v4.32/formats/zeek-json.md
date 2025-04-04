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

# Synoposis

```
zeek-json [--schema-only] [--raw] [--no-infer]
```

### Common Options (Parser)

The Suricata parser supports some of the common [schema inference options](formats.md#parser-schema-inference).
