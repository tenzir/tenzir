# SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause

setup_db() {
  export TENZIR_DB_DIRECTORY="tenzir-functional-test-state/$BATS_TEST_NAME"
  rm -rf "$TENZIR_DB_DIRECTORY"
}
teardown_db() {
  if [ -n  "$BATS_TEST_COMPLETED" ] && [  "$BATS_TEST_COMPLETED" -eq 1 ]; then
    rm -rf "$TENZIR_DB_DIRECTORY"
  fi
}

setup_node() {
  # The exec is needed so that signals to $NODE_PID actually reach the node.
  coproc NODE { exec tenzir-node -e ":0" --print-endpoint; }
  read -r -u "${NODE[0]}" TENZIR_ENDPOINT
  export TENZIR_ENDPOINT
}
teardown_node() {
  kill "$NODE_PID"
  # Hard kill the node after a bit of time.
  local seconds=5
  { sleep ${seconds} && { debug 0 "killing the node after ${seconds} second shutdown timeout"; kill -9 "$NODE_PID"; }; } &
  local killerPid=$!
  wait "$NODE_PID"
  # The sleep is a child process of the killer shell, so we have to use
  # `pkill -P`.
  pkill -P "$killerPid"
}
