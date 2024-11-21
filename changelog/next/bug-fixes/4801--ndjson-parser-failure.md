The `read_ndjson` operator no longer uses an error-prone mechanism to continue
parsing an NDJSON line that contains an error. Instead, the entire line is
skipped.
