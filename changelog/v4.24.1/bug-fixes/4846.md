We fixed a rare crash on startup that would occur when starting the
`tenzir-node` process was so slow that it would try to emit metrics before the
component handling metrics was ready.
