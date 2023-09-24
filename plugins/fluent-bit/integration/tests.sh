#! /usr/bin/env bash

set -euxo pipefail

tenzir() {
  "${app}" --bare-mode --plugins=fluent-bit exec "$@"
}

# Source
# ======

# Do not crash on invalid Fluent Bit plugin.
tenzir 'fluent-bit please-do-not-crash' || true

# Check that we can generate *something* via the random input.
tenzir 'fluent-bit random | head 1 | put schema=#schema'

# Leverage Fluent Bit's stdin plugin.
echo '{"foo": {"bar": 42}}' | tenzir 'fluent-bit stdin | drop timestamp'

# Sink
# ====

# Equivalent to our `discard` operator.
tenzir 'show operators | fluent-bit null'

# Produce a single line of output. Then trim the timestamp for determinism.
tenzir 'show operators | where name == "unique" | fluent-bit stdout' |
  cut -d ' ' -f 5-

# Use the counter output to show 10 results. Then trim the timestamp for
# deterministic output.
tenzir 'show operators | head | fluent-bit counter' |
  cut -d , -f 2
