# Complex types with local scope.

event foo
(
    e: enum { x, y, z },
    v: vector[addr],
    s: set[pattern],
    t: table[port] of addr
)
