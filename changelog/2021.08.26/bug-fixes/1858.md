The CSV parser now correctly parses quoted fields in non-string types. E.g.,
`"127.0.0.1"` in CSV now successfully parsers when a matching schema contains
an `address` type field.
