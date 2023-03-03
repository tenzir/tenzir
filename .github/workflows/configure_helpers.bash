# Utility alias for detecting changes to files.
shopt -s expand_aliases
alias is_changed="! git diff --quiet --exit-code ${before_sha} --"

run_if_changed_default() {
  local job="$1"
  local default="$2"
  shift
  shift
  job_shellvar="run_$(echo "$job" | tr '-' '_')"
  if is_changed "${@}"; then
    declare $job_shellvar="true"
    echo "run-${job}=true" >> $GITHUB_OUTPUT
  else
    declare $job_shellvar="false"
  fi
}

run_if_changed() {
  local job="$1"
  shift
  run_if_changed_default "$job" false "$@"
}

any() {
  local result=false
  for x in "$@"; do
    if [[ $x == "true" ]]; then
      result=true
    fi
  done
  echo "$result"
}
