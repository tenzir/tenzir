VAST no longer vendors [xxHash](https://github.com/Cyan4973/xxHash), which is
now a regular required dependency. Internally, VAST switched its default hash
function to XXH3, providing a speedup of up to 3x.
