# Complex types with local scope.

type foo = record
{
    e: enum { x, y, z },
    v: vector<addr>,
    s: set<pattern>,
    t: table<port, addr>
}
