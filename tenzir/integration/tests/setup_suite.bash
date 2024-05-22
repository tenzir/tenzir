setup_suite() {
  bats_require_minimum_version 1.8.0
  bats_load_library bats-tenzir

  bats_tenzir_initialize

  export INPUTSDIR="${BATS_TENZIR_INPUTSDIR}"
  export QUERYDIR="${BATS_TENZIR_QUERIESDIR}"
  export MISCDIR="${BATS_TENZIR_MISCDIR}"
}

teardown_suite() {
  : # Nothing to do
}

if ! command -v tenzir; then
  echo "tenzir binaries must be on $PATH"
  return 1
fi

libpath_relative_to_binary="$(realpath "$(dirname "$(command -v tenzir)")")/../share/tenzir/integration/lib"
libpath_relative_to_pwd="${BATS_TEST_DIRNAME%%/integration/*}/integration/lib"
export BATS_LIB_PATH=${libpath_relative_to_binary}:${libpath_relative_to_pwd}${BATS_LIB_PATH:+:${BATS_LIB_PATH}}
echo ${BATS_LIB_PATH}
