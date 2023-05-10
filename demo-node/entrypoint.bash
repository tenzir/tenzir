#!/usr/bin/env bash

coproc NODE { exec vast start --print-endpoint; }
# shellcheck disable=SC2034
read -r -u "${NODE[0]}" DUMMY

replay_zeek() {
  :
}
export replay_zeek

curl -L https://storage.googleapis.com/vast-datasets/M57/suricata.tar.zst | tar -x --zstd --to-command="vast import suricata" &

curl -L https://storage.googleapis.com/vast-datasets/M57/zeek.tar.zst | tar -x --zstd
find Zeek -type f -print0 | xargs --null -P 99 -I{} sh -c "export VAST_CONSOLE_FORMAT='{}: %^[%T.%e] %v%\$'; /demo-node/timeshift-zeek.bash {} | vast -e localhost import zeek"

wait "$NODE_PID"
