VAST's internal data model now preserves the nesting of the stored data
completely when using the `arrow` encoding, and maps the pattern, address,
subnet, and enumeration types onto Arrow extension types rather than using the
underlying representation directly. This change enables the use of the `export
arrow` command without needing information about VAST' type system.

Transform steps that add or modify columns now add or modify the columns
in-place rather than at the end, preserving the nesting structure of the
original data.

The deprecated `msgpack` encoding no longer exists. Data imported using the
`msgpack` encoding can still be accessed, but new data will now always use the
`arrow` encoding.
