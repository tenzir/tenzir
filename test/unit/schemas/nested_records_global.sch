# A hierarchy of three nested records, each of which have global scope.

type inner = record
{
    a: addr,
    p: port
}

type middle = record
{
    b: bool,
    i: inner
}

type outer = record
{
    a: addr,
    j: middle
}

type foo = record{r: outer}
