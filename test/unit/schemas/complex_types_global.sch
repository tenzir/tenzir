# Complex types with global scope.

type enum_t = enum { x, y, z }

type vector_t = vector<addr>

type set_t = set<pattern>

type table_t = table<port, addr>

type foo = record
{
    e: enum_t,
    v: vector_t,
    s: set_t,
    t: table_t
}
