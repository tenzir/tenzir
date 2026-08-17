#!/usr/bin/env bash
set -euo pipefail

count="${1:-100000}"
variant="${BENCHMARK_VARIANT:-raw}"
tenzir_bin="${TENZIR_BIN:-tenzir}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
container="tenzir-rsyslog-bench-$$"
results="$root/results"
mkdir -p "$results"

is_running() {
  local state
  state="$(ps -o stat= -p "$1" 2>/dev/null | xargs)"
  [[ -n "$state" && "$state" != Z* ]]
}

cleanup() {
  if [[ -n "${tenzir_pid:-}" ]] && is_running "$tenzir_pid"; then
    kill "$tenzir_pid" 2>/dev/null || true
  fi
  if [[ -n "${timer_pid:-}" ]] && is_running "$timer_pid"; then
    kill "$timer_pid" 2>/dev/null || true
    wait "$timer_pid" 2>/dev/null || true
  fi
  docker rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker build --quiet --tag tenzir-rsyslog-bench "$root" >/dev/null
docker run --detach --rm \
  --name "$container" \
  --add-host host.docker.internal:host-gateway \
  --publish 127.0.0.1:1514:1514 \
  --volume "$root/rsyslog.conf:/etc/rsyslog.conf:ro" \
  tenzir-rsyslog-bench >/dev/null

case "$variant" in
  raw)
    cat >"$results/pipeline.tql" <<EOF
accept_relp "0.0.0.0:2514"
head $count
summarize events=count()
EOF
    ;;
  parsed)
    cat >"$results/pipeline.tql" <<EOF
let \$openssh = r"Failed password for (?:invalid user )?%{USERNAME:user} from %{IP:source_ip} port %{POSINT:source_port:int} %{WORD:protocol}"
accept_relp "0.0.0.0:2514"
syslog = data.parse_syslog()
parsed = syslog.message.parse_grok(\$openssh)
where parsed.user == "admin"
head $count
summarize events=count()
EOF
    ;;
  *)
    echo "unknown BENCHMARK_VARIANT: $variant (expected raw or parsed)" >&2
    exit 2
    ;;
esac

resource_file="$results/resources.txt"
if [[ "$(uname -s)" == "Darwin" ]]; then
  time_args=(-lp -o "$resource_file")
else
  time_args=(-v -o "$resource_file")
fi
tenzir_args=(--bare-mode)
if [[ "${BENCHMARK_DIAGNOSTICS:-false}" == "true" ]]; then
  tenzir_args=(-vv "${tenzir_args[@]}")
fi
/usr/bin/time "${time_args[@]}" \
  "$tenzir_bin" "${tenzir_args[@]}" -f "$results/pipeline.tql" \
  >"$results/tenzir-output.txt" 2>"$results/tenzir-stderr.txt" &
timer_pid=$!
for _ in $(seq 1 300); do
  tenzir_pid="$(pgrep -P "$timer_pid" || true)"
  if [[ -n "$tenzir_pid" ]]; then
    break
  fi
  sleep 0.01
done
if [[ -z "${tenzir_pid:-}" ]]; then
  echo "Tenzir process did not start" >&2
  exit 1
fi
python3 - "$tenzir_pid" <<'PY'
import os
import socket
import sys
import time

pid = int(sys.argv[1])
for _ in range(3_000):
    try:
        with socket.create_connection(("127.0.0.1", 2514), timeout=0.01):
            break
    except OSError:
        try:
            os.kill(pid, 0)
        except OSError:
            raise SystemExit("Tenzir exited before opening the RELP listener")
        time.sleep(0.01)
else:
    raise SystemExit("Tenzir did not open the RELP listener")
PY

started_ns="$(python3 -c 'import time; print(time.monotonic_ns())')"
python3 "$root/send.py" "$count" >"$results/sender.json"

for _ in $(seq 1 1200); do
  if ! is_running "$tenzir_pid"; then
    break
  fi
  sleep 0.1
done
if is_running "$tenzir_pid"; then
  echo "Tenzir did not finish within 120 seconds" >&2
  exit 1
fi
wait "$timer_pid"
unset tenzir_pid timer_pid
ended_ns="$(python3 -c 'import time; print(time.monotonic_ns())')"

python3 - \
  "$variant" "$count" "$started_ns" "$ended_ns" "$results/sender.json" \
  "$resource_file" <<'PY'
import json
import pathlib
import re
import sys

variant = sys.argv[1]
count = int(sys.argv[2])
elapsed = (int(sys.argv[4]) - int(sys.argv[3])) / 1_000_000_000
sender = json.loads(pathlib.Path(sys.argv[5]).read_text())
resources = pathlib.Path(sys.argv[6]).read_text()

def match(pattern: str) -> float | None:
    found = re.search(pattern, resources, re.MULTILINE)
    return float(found.group(1)) if found else None

user_seconds = match(r"^user\s+([0-9.]+)$")
system_seconds = match(r"^sys\s+([0-9.]+)$")
max_resident_bytes = match(r"^\s*([0-9]+)\s+maximum resident set size$")
if user_seconds is None:
    user_seconds = match(r"^\s*User time \(seconds\):\s*([0-9.]+)$")
if system_seconds is None:
    system_seconds = match(r"^\s*System time \(seconds\):\s*([0-9.]+)$")
if max_resident_bytes is None:
    max_resident_kib = match(r"^\s*Maximum resident set size \(kbytes\):\s*([0-9]+)$")
    max_resident_bytes = max_resident_kib * 1024 if max_resident_kib else None

result = {
    "variant": variant,
    **sender,
    "end_to_end_seconds": elapsed,
    "events_per_second": count / elapsed,
    "payload_megabytes_per_second": sender["input_bytes"] / elapsed / 1_000_000,
    "cpu_user_seconds": user_seconds,
    "cpu_system_seconds": system_seconds,
    "max_resident_bytes": int(max_resident_bytes) if max_resident_bytes else None,
}
print(json.dumps(result, indent=2))
PY
cat "$results/tenzir-output.txt"
