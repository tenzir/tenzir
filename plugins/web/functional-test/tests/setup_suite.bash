setup_suite() {
  bats_require_minimum_version 1.8.0
}

TENZIR_DIR="$(realpath "$(dirname "$(command -v tenzir)")")"
export BATS_LIB_PATH=${BATS_LIB_PATH:+${BATS_LIB_PATH}:}${TENZIR_DIR}/../share/tenzir/functional-test/lib:${BATS_TEST_DIRNAME}/../../../../tenzir/functional-test/lib

bats_load_library bats-tenzir

setup_node_with_web_plugin() {
  export_default_node_config
  export TENZIR_PLUGINS="web"
  set_node_raw
}

