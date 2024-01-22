The `zeek-tsv` printer incorrectly emitted metadata too frequently. It now only
writes opening and closing tags when it encounters a new schema.
