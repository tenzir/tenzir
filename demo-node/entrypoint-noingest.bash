#!/usr/bin/env bash

export TENZIR_AUTOMATIC_REBUILD="${TENZIR_AUTOMATIC_REBUILD:-0}"
export TENZIR_ALLOW_UNSAFE_PIPELINES=true

# Forward signals as SIGTERM to all children.
trap 'trap " " SIGTERM; kill 0; wait' SIGINT SIGTERM

coproc NODE { exec tenzir-node --print-endpoint --commands="web server --mode=dev"; }
# shellcheck disable=SC2034
read -r -u "${NODE[0]}" DUMMY

# Reset the verbosity so the logs don't get spammed by dozens
# of Tenzir processes.
export TENZIR_CONSOLE_VERBOSITY="${TENZIR_CLIENT_CONSOLE_VERBOSITY:-"info"}"

tenzir 'remote version | put version | write json'

wait "$NODE_PID"
