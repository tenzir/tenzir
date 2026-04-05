#!/usr/bin/env bash

set -euo pipefail

export LC_ALL=C

preset="nix-clang-debug"
target="tenzir"
incremental_file="libtenzir/builtins/formats/json.cpp"
out_dir="./build/benchmarks"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "error: run this script from inside a git repository" >&2
  exit 1
fi

if [[ $# -ne 0 ]]; then
  echo "error: this script does not accept arguments" >&2
  exit 1
fi

if [[ -z $out_dir ]]; then
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  out_dir="build/benchmarks/compile-times-${ts}"
fi

mkdir -p "$out_dir"
results_tsv="${out_dir}/results.tsv"
printf "scenario\tseconds\tfile\n" >"$results_tsv"

echo "==> configuring preset '${preset}'"
cmake --preset "$preset" >/dev/null

run_build() {
  local scenario="$1"
  local file="$2"
  local slug
  slug="$(printf '%s' "$scenario" | tr ' /:' '___')"
  local log_file="${out_dir}/${slug}.log"
  local time_file="${out_dir}/${slug}.time"

  echo "==> ${scenario} build"
  if ! {
    TIMEFORMAT='%R'
    time cmake --build --preset "$preset" --target "$target" >"$log_file" 2>&1
  } 2>"$time_file"; then
    echo "error: build failed for scenario '${scenario}'" >&2
    echo "log: ${log_file}" >&2
    exit 1
  fi
  local seconds
  seconds="$(tr -d '[:space:]' <"$time_file")"

  printf "%s\t%s\t%s\n" "$scenario" "$seconds" "$file" >>"$results_tsv"
  echo "  ${seconds}s"
}

echo "==> cleaning"
cmake --build --preset "$preset" --target clean >"${out_dir}/clean-target.log" 2>&1
run_build "clean" ""

echo "==> touching ${incremental_file}"
touch "$incremental_file"
run_build "incremental" "$incremental_file"

echo "==> summary"
while IFS=$'\t' read -r scenario seconds file; do
  [[ $scenario == "scenario" ]] && continue
  printf '%-15s %8ss  %s\n' "$scenario" "$seconds" "$file"
done <"$results_tsv"

echo -e "\nresults: ${results_tsv}"
