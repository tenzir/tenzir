A regression caused VAST's plugins to be loaded in a random order, which
caused a warning about mismatching plugins between client and server to
be emitted randomly when connecting to a VAST server. This is now fixed.
