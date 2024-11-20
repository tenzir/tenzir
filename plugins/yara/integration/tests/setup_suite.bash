setup_suite() {
  bats_require_minimum_version 1.8.0
  bats_load_library bats-tenzir

  bats_tenzir_initialize
}

libpath_relative_to_binary="$(realpath "$(dirname "$(command -v tenzir)")")/../share/tenzir/integration/lib"
libpath_relative_to_pwd="${BATS_TEST_DIRNAME}/../../../../lib"
libpath_relative_if_external=${BATS_TEST_DIRNAME}/../../../../tenzir/integration/lib #needed for the externalized build in docker
export BATS_LIB_PATH=${BATS_LIB_PATH:+${BATS_LIB_PATH}:}${libpath_relative_to_binary}:${libpath_relative_to_pwd}:${libpath_relative_if_external}
