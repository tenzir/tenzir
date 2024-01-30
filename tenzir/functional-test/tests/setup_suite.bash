setup_suite() {
  bats_require_minimum_version 1.8.0
  bats_load_library bats-tenzir

  export_default_paths
  export_default_node_config
  
  export INPUTSDIR="${BATS_TENZIR_INPUTSDIR}"
  export QUERYDIR="${BATS_TENZIR_QUERIESDIR}"
  export MISCDIR="${BATS_TENZIR_MISCDIR}"
}

teardown_suite() {
  : # Nothing to do
}

if ! which tenzir; then
  echo "tenzir binaries must be on $PATH"
  return 1
fi

# Try to load the `bats-tenzir` library either relative to
# the current directory (for in-tree builds) or relative to the
# tenzir binary (for builds against an installed tenzir)
TENZIR_DIR="$(realpath "$(dirname "$(command -v tenzir)")")"
export BATS_LIB_PATH=${BATS_LIB_PATH:+${BATS_LIB_PATH}:}${TENZIR_DIR}/../share/tenzir/functional-test/lib:${BATS_TEST_DIRNAME}/../../../../../tenzir/functional-test/lib
