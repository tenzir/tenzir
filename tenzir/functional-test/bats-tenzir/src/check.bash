# SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause

# TODO: Add documentation.
check() {
  local expect_error=0
  local sort_output=0
  local with_stderr=0
  local args=()
  if [[ -v step_nr_ ]]; then
    step_nr_=$(( step_nr_ + 1 ))
  else
    step_nr_=0
  fi
  local step
  step="step_$(printf '%02d' "${step_nr_}")"
  export step

  while [[ $# -gt 0 ]]; do
    case $1 in
      '!')
        shift # past argument
        # TODO: compare against stderr instead of stdout
        expect_error=1
        ;;
      --with-stderr)
        with_stderr=1
        shift # past argument
        ;;
      --sort)
        sort_output=1
        shift # past argument
        ;;
      --bg)
        shift # past argument
        local process_group="$1"
        shift # past value
        ;;
      -c|--cmd)
        shift # past argument
        args+=("bash")
        args+=("-c")
        args+=("$1")
        shift # past value
        ;;
      *)
        args+=("$1") # save positional arg
        shift # past argument
        ;;
    esac
  done
  run_flags=()
  if [ $expect_error = 1 ]; then
    run_flags+=("!")
  else
    run_flags+=("-0")
  fi
  if [ $with_stderr = 0 ]; then
    run_flags+=("--separate-stderr")
  fi
  compare_or_update() {
    local file_ref_dir ref_path
    # --relative-to is not available on macOS. We can assume that the suite dir is a
    # prefix to the test file path instead.
    # file_ref_dir="$(realpath --relative-to="${BATS_SUITE_DIRNAME}" "${BATS_TEST_FILENAME%.bats}")"
    file_ref_dir="${BATS_TEST_FILENAME%.bats}"
    file_ref_dir="${file_ref_dir#"$BATS_SUITE_DIRNAME"}"
    ref_path="$(dirname "${BATS_SUITE_DIRNAME}")/reference/${file_ref_dir}/${BATS_TEST_NAME}"
    if [[ -v UPDATE ]]; then
      debug 0 "creating ${ref_path}"
      mkdir -p "${ref_path}"
      if [[ "$step_nr_" -eq 0 ]]; then
        rm -f "${ref_path}/*"
      fi
      if [[ -n "${output}" ]]; then
        if [ $sort_output = 1 ]; then
          output="$(printf '%s' "${output}" | LC_ALL=C sort)"
        fi
        if [[ -f "${ref_path}/${step}.ref" ]]; then
          local expected
          expected="$(<"${ref_path}/${step}.ref")"
          if [[ $output != "$expected" ]]; then
            debug 0 "updating ${ref_path}/${step}.ref"
            printf '%s' "${output}" > "${ref_path}/${step}.ref"
          else
            debug 1 "keeping ${ref_path}/${step}.ref"
          fi
        else
          debug 0 "creating ${ref_path}/${step}.ref"
          printf '%s' "${output}" > "${ref_path}/${step}.ref"
        fi
      fi
    else
      debug 1 "checking against ${ref_path}/${step}.ref"
      if [[ -f "${ref_path}/${step}.ref" ]]; then
        if [ $sort_output = 1 ]; then
          assert_sorted - < "${ref_path}/${step}.ref"
        else
          assert_output - < "${ref_path}/${step}.ref"
        fi
      else
        refute_output
      fi
    fi
  }
  debug 1 "running: ${args[*]}"
  if [ -n "${process_group}" ]; then
    declare -n pg="$process_group"
    {
      run "${run_flags[@]}" -- "${args[@]}" 3>&-
      compare_or_update
    } &
    pg+=($!)
  else
    run "${run_flags[@]}" -- "${args[@]}"
    compare_or_update
  fi
}

check_jq() {
  # FIXME
  check -c "$1 | jq -ec ."
}

wait_all() {
  for pid in "$@"; do
    wait "$pid" || exit 1
  done
}
