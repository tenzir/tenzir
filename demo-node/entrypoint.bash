#!/usr/bin/env bash

coproc NODE { exec tenzir-node --print-endpoint; }
# shellcheck disable=SC2034
read -r -u "${NODE[0]}" DUMMY

# Reset the verbosity so the logs don't get spammed by dozens
# of Tenzir processes.
export TENZIR_CONSOLE_VERBOSITY=info

# Allow unrestricted access, so we can run `from file` or `shell` operators.
export TENZIR_ALLOW_UNSAFE_PIPELINES=true

replay_zeek() {
  :
}
export replay_zeek

curl -L https://storage.googleapis.com/tenzir-datasets/M57/suricata.tar.zst | tar -x --zstd --to-command="vast import suricata" &

curl -L https://storage.googleapis.com/tenzir-datasets/M57/zeek.tar.zst | tar -x --zstd
find Zeek -type f -print0 | xargs --null -P 99 -I{} sh -c "export VAST_CONSOLE_FORMAT='{}: %^[%T.%e] %v%\$'; /demo-node/timeshift-zeek.bash {} | tenzir-ctl -e localhost import zeek"

wait "$NODE_PID"
