setup_suite() {
  bats_require_minimum_version 1.8.0

  mkdir -p tenzir-functional-test-state
}

teardown_suite() {
  # coreutils rmdir has --ignore-fail-on-non-empty, but that is not portable.
  # We simply assume that an error means the directory is not empty.
  rmdir tenzir-functional-test-state || true
}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export BATS_LIB_PATH=${BATS_LIB_PATH:+${BATS_LIB_PATH}:}${SCRIPT_DIR}/../

BATS_SUITE_DIRNAME="${BATS_TEST_DIRNAME}"
export BATS_SUITE_DIRNAME

unset "${!TENZIR@}"
# Enable bare mode so settings in ~/.config/tenzir or the build configuration
# have no effect.
export TENZIR_BARE=1
export TENZIR_PLUGINS=""
