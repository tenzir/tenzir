setup_suite() {
  bats_require_minimum_version 1.8.0

  bats_load_library bats-tenzir
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

echo "setup suite is executed!"

# Enable bare mode so settings in ~/.config/tenzir or the build configuration
# have no effect.
export TENZIR_BARE_MODE=1
export TENZIR_PLUGINS=""
# TODO export TENZIR_PLUGINS__JSON__COMPACT_OUTPUT=true

# TODO: Rename to ${BATS_TENZIR_SUITE_DATADIR} and move to bats-tenzir library?
export BATS_TENZIR_DATADIR="$(dirname ${BATS_TEST_DIRNAME})/data"
