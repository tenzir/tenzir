 The `xsv` parser (and by extension the `csv`, `tsv`, and `ssv` parsers) skipped
 lines that had a mismatch between the number of values contained and the number
 of fields defined in the header. Instead, it now fills in `null` values for
 missing values and adds new fields for missing headers.
