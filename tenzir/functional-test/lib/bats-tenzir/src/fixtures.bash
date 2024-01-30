# SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause


export_default_node_config() {
  export TENZIR_STATE_DIRECTORY="${BATS_TEST_TMPDIR}/db"
  export TENZIR_BARE_MODE=true
  export TENZIR_PLUGINS=""
  export TENZIR_ENDPOINT=":0"
  export TENZIR_EXPORT__ZEEK__DISABLE_TIMESTAMP_TAGS=true
  export TENZIR_AUTOMATIC_REBUILD=0
  export TENZIR_EXEC__DUMP_DIAGNOSTICS=true
  export TENZIR_EXEC__IMPLICIT_EVENTS_SINK="write json --compact-output | save -"
  export TENZIR_ENABLE_METRICS=false
  export TENZIR_ALLOW_UNSAFE_PIPELINES="true"
}

setup_node_raw() {
  # Print node config
  set | grep -Ee "^TENZIR" || true >&3
  # The inner exec is needed so that signals to $NODE_PID actually reach the
  # node.
  local node_args=$1
  exec {NODE_OUT}< <(exec tenzir-node --print-endpoint ${node_args})
  NODE_PID=$!
  read -r -u "$NODE_OUT" TENZIR_ENDPOINT
  export TENZIR_ENDPOINT
}

setup_node_with_plugins() {
  local plugins=$1
  export_default_node_config
  export TENZIR_PLUGINS="$plugins"
  setup_node_raw
}

# Start a node with a configuration suitable for most integration tests.
setup_node() {
  export_default_node_config
  setup_node_raw
}

teardown_node() {
  kill "$NODE_PID"
  # Hard kill the node after a bit of time.
  local seconds=5
  { sleep ${seconds} && { debug 0 "killing the node after ${seconds} second shutdown timeout"; kill -9 "$NODE_PID"; }; } &
  local killerPid=$!
  wait "$NODE_PID"
  # Some FS writes may still be buffered, and they would lead the subsequent
  # cleanup logic astray, so we flush them out here.
  sync
  # The sleep is a child process of the killer shell, so we have to use
  # `pkill -P`.
  pkill -P "$killerPid"
  # This closes the fd attached to stdout on the reading side for good measure.
  exec {NODE_OUT}<&-
}
