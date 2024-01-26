setup_suite() {
  bats_require_minimum_version 1.8.0
}

TENZIR_DIR="$(realpath "$(dirname "$(command -v tenzir)")")"
export BATS_LIB_PATH=${BATS_LIB_PATH:+${BATS_LIB_PATH}:}${TENZIR_DIR}/../share/tenzir/functional-test/lib:${BATS_TEST_DIRNAME}/../../../../tenzir/functional-test/lib

# Enable bare mode so settings in ~/.config/tenzir or the build configuration
# have no effect.
bats_load_library bats-tenzir
export_default_node_config
export TENZIR_PLUGINS="fluent-bit"
