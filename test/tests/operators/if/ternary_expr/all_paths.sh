#!/bin/sh
# runner: shell
set -eu

dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

run_case() {
  mode="$1"
  name="$2"
  case_file="$3"
  printf '=== %s: %s ===\n' "$mode" "$name"
  if [ "$mode" = "eager" ]; then
    sh -c "TENZIR_EXEC__DUMP_DIAGNOSTICS=false TENZIR_EVAL_DISABLE_SHORT_CIRCUITING=1 $TENZIR_BINARY --bare-mode --console-verbosity=warning --multi -f \"$case_file\"" 2>/dev/null
  else
    sh -c "TENZIR_EXEC__DUMP_DIAGNOSTICS=false $TENZIR_BINARY --bare-mode --console-verbosity=warning --multi -f \"$case_file\"" 2>/dev/null
  fi
}

for mode in default eager; do
  run_case "$mode" "all_true" "$dir/case_all_true.tql.in"
  run_case "$mode" "all_false" "$dir/case_all_false.tql.in"
  run_case "$mode" "mixed_same_type" "$dir/case_mixed_same_type.tql.in"
  run_case "$mode" "mixed_different_type" "$dir/case_mixed_different_type.tql.in"
  run_case "$mode" "null_predicate" "$dir/case_null_predicate.tql.in"
  run_case "$mode" "non_bool_predicate" "$dir/case_non_bool_predicate.tql.in"
done
