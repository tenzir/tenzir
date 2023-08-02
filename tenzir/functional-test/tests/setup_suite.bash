setup_suite() {
  bats_require_minimum_version 1.8.0

  mkdir -p tenzir-functional-test-state
}

teardown_suite() {
  # Remove the state dir if all tests cleaned up after themselves.
  if [ "$(ls -A tenzir-functional-test-state)" == "" ]; then
    rmdir tenzir-functional-test-state
  else
    debug 0 "Keeping tenzir-functional-test-state directory."
  fi
}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export BATS_LIB_PATH=${BATS_LIB_PATH:+${BATS_LIB_PATH}:}${SCRIPT_DIR}/../

BATS_SUITE_DIRNAME="${BATS_TEST_DIRNAME}"
export BATS_SUITE_DIRNAME

unset "${!TENZIR@}"
# Enable bare mode so settings in ~/.config/tenzir or the build configuration
# have no effect.
export TENZIR_BARE_MODE=1
export TENZIR_PLUGINS=""
