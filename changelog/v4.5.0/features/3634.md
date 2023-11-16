In `where <expression>`, the types of numeric literals and numeric fields in an
equality or relational comparison must no longer match exactly. The literals
`+42`, `42` or `42.0` now compare against fields of types `int64`, `uint64`, and
`double` as expected.
