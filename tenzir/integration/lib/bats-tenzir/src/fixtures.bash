# SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause

export_default_paths() {
  BATS_TEST_DATADIR="$(realpath "$(dirname "${BATS_TEST_DIRNAME}")")"
  # Look for the first folder called `integration/` that is a parent
  # of the location of the test file.
  export BATS_TENZIR_DATADIR="${BATS_TEST_DATADIR%%/integration/*}/data"

  export BATS_TENZIR_INPUTSDIR="${BATS_TENZIR_DATADIR}/inputs"
  export BATS_TENZIR_QUERIESDIR="${BATS_TENZIR_DATADIR}/queries"
  export BATS_TENZIR_MISCDIR="${BATS_TENZIR_DATADIR}/misc"
  export BATS_TENZIR_CONFIGDIR="${BATS_TENZIR_DATADIR}/config"
}

export_default_node_config() {
  export TENZIR_BARE_MODE=true
  export TENZIR_STATE_DIRECTORY="${BATS_TEST_TMPDIR}/db"
  export TENZIR_PLUGINS=""
  export TENZIR_ENDPOINT=":0"
  export TENZIR_EXPORT__ZEEK__DISABLE_TIMESTAMP_TAGS=true
  export TENZIR_AUTOMATIC_REBUILD=0
  export TENZIR_EXEC__DUMP_DIAGNOSTICS=true
  export TENZIR_EXEC__IMPLICIT_EVENTS_SINK="write json --compact-output | save -"
}

bats_tenzir_initialize() {
  # Normalize the environment unless `BATS_TENZIR_KEEP_ENVIRONMENT` is set.
  if [ -z "${BATS_TENZIR_KEEP_ENVIRONMENT}" ]; then
    # shellcheck disable=SC2046
    unset $(printenv | grep -o '^TENZIR[^=]*' | paste -s -)
  fi

  export_default_paths
  export_default_node_config
}

setup_node() {
  # Always enforce bare mode even when using custom config.
  export TENZIR_BARE_MODE=true
  # Print node config
  set | grep -Ee "^TENZIR" || true >&3
  # The inner exec is needed so that signals to $NODE_PID actually reach the
  # node.
  exec {NODE_OUT}< <(exec tenzir-node --print-endpoint "${@}")
  NODE_PID=$!
  read -r -u "$NODE_OUT" TENZIR_ENDPOINT
  export TENZIR_ENDPOINT
}

setup_node_with_plugins() {
  local plugins=$1
  export_default_node_config
  export TENZIR_PLUGINS="$plugins"
  setup_node "${@:1}"
}

# Start a node with a configuration suitable for most integration tests.
setup_node_with_default_config() {
  export_default_node_config
  setup_node
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
