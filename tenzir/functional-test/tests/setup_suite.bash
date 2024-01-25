setup_suite() {
  bats_require_minimum_version 1.8.0
  bats_load_library bats-tenzir

  # The default node config is also reasonable for the client commands, ie.
  # enabling bare mode and disabling plugins etc.
  export_default_node_config
}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export BATS_LIB_PATH=${BATS_LIB_PATH:+${BATS_LIB_PATH}:}${SCRIPT_DIR}/../lib

BATS_SUITE_DIRNAME="${BATS_TEST_DIRNAME}"
export BATS_SUITE_DIRNAME

# Normalize the environment unless `BATS_KEEP_ENVIRONMENT` is set.
if [[ ! -n ${BATS_KEEP_ENVIRONMENT} ]]; then
  unset $(printenv | grep -o '^TENZIR[^=]*' | paste -s -)
fi

if ! which tenzir-node; then
  echo "tenzir binaries must be on $PATH"
  return 1
fi

# TODO: Should the datadir definitions move into the bats-tenzir library,
# so that files in there automatically available for plugins integration tests?
export BATS_TENZIR_DATADIR="$(dirname ${BATS_TEST_DIRNAME})/data"

export INPUTSDIR="${BATS_TENZIR_DATADIR}/inputs"
export QUERYDIR="${BATS_TENZIR_DATADIR}/queries"
export MISCDIR="${BATS_TENZIR_DATADIR}/misc"

