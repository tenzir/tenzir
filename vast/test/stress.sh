#!/usr/bin/env bash

# Warns about variable expansions in single quotes.
# shellcheck disable=SC2016

set -euo pipefail

log() { printf "%s\n" "$*" >&2; }

declare -a on_exit=()
cleanup() {
  log running ${#on_exit[@]} cleanup steps
  for (( i = 0; i < ${#on_exit[@]}; i++ )); do
    eval "log \"[cleanup $i] ${on_exit[$i]}\""
    eval "${on_exit[$i]}"
  done
}
trap '{
  log got SIGINT
}' INT
trap '{
  log got SIGTERM
}' TERM
trap '{
  log got EXIT
  cleanup
}' EXIT


: "${VAST:="vast"}"
: "${DB_DIR:="$(mktemp -p "${PWD}" -d vast-stress.XXXXXXXX)"}"

dir="$(dirname "$(readlink -f "$0")")"
pushd "${dir}"
on_exit=('popd; rm -rf ${DB_DIR}' "${on_exit[@]}")

coproc nodefd {
  "${VAST}" --config="${dir}/stress.yaml" -e 0.0.0.0:0 -d "${DB_DIR}" start --print-endpoint 2> >(sed "s/^/[node] /" >&2)
}
IFS=: read -u "${nodefd[0]}" -r addr port
# node is listening now
log "endpoint is ligening on ${addr}:${port}"
endpoint="localhost:$port"

on_exit=('"${VAST}" -e "${endpoint}" stop' "${on_exit[@]}")

vast() {
  "${VAST}" "$@"
}

# Using a file as a global variable for the return code. If any vast child
# process fails the content is set to "1".
RETCODE_STORE="$(mktemp)"
echo 0 > "${RETCODE_STORE}"

collect_retcode() {
  read -r code < "${RETCODE_STORE}"
  rm "${RETCODE_STORE}"
  exit "${code}"
}
on_exit=("${on_exit[@]}" 'collect_retcode')

client() {
  vast -e "${endpoint}" "$@" || echo 1 > "${RETCODE_STORE}"
}

# TODO: Consider using `systemd-run --scope --unit=stress ./stress.sh` to clean
# up children in case of an interrupt.

(
while read -rd $'\0' path; do
  IFS=/ read -r _ _ _ format _ <<< "${path}"
  echo ingesting "${path}"
  # import -b deadlocks the script
  zcat "${path}" | client import "${format}" &
done < <(find ../integration/data -name '*.gz' -print0)
client export null '#timestamp < 10 days ago' &
client status --detailed
client export null '#timestamp < 10 days ago' &
client export null '#timestamp < 10 days ago' &
client export null '#timestamp < 10 days ago' &
client export null '#timestamp < 10 days ago' &
client export null '#timestamp < 10 days ago' &
client export null '#timestamp < 10 days ago' &
client export null '#timestamp < 10 days ago' &
client export null '#timestamp < 10 days ago' &

wait
)
