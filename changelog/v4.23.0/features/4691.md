The relational operator `in` now supports checking for existence of an element
in a list. For example, `where x in ["important", "values"]` is functionally
equivalent to `where x == "important" or x == "values"`.
