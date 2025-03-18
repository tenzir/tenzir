The `read_zeek_tsv` operator sometimes produced an invalid field with the name
`\0` for types without a schema specified. This no longer happens.
