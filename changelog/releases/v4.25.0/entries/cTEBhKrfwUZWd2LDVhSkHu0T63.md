---
title: "Custom quotes and doubled quote escaping"
type: feature
author: IyeOnline
created: 2025-01-07T16:17:24Z
pr: 4837
---

The `read_csv`, `read_kv`, `read_ssv`, `read_tsv` and `read_xsv` operators now
support custom quote characters.

The `read_csv`, `read_ssv`, `read_tsv` and `read_xsv` operators support doubled
quote escaping.

The `read_csv`, `read_ssv`, `read_tsv` and `read_xsv` operators now accept
multi-character strings as separators.

The `list_sep` option for the `read_csv`, `read_ssv`, `read_tsv` and `read_xsv`
operators can be set to an empty string, which will disable list parsing.

The new `string.parse_leef()` function can be used to parse a string as a LEEF
message.
