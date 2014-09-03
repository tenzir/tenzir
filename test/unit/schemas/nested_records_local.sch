# Nested records with local scope.

type foo: record{r: record{a: addr, i: record{b: bool}}}
type bar: record{r: record{s: record{t: record{x: int}}}}
