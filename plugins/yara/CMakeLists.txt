cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  yara
  DESCRIPTION "Yara plugin for Tenzir"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside Tenzir, it is necessary to specify this
# line manually so plugins can be linked against an installed Tenzir.
include(CTest)

find_package(Tenzir REQUIRED)
TenzirRegisterPlugin(
  TARGET yara ENTRYPOINT src/plugin.cpp
)

find_package(PkgConfig REQUIRED QUIET)
pkg_check_modules(yara REQUIRED IMPORTED_TARGET yara)
dependency_summary("libyara" PkgConfig::yara "Dependencies")
target_link_libraries(yara PUBLIC PkgConfig::yara)

# libyara.a needs flags indicated by the `.private` pkg-config sections, but
# those are not exposed as part of the IMPORTED_TARGET.
if (TENZIR_ENABLE_STATIC_EXECUTABLE)
  # NOTE: Despite the claims of the docs, `XXX_STATIC_LINK_LIBRARIES`
  # doesn't exist, so we have to use `yara_STATIC_LIBRARIES`.
  target_link_libraries(PkgConfig::yara INTERFACE ${yara_STATIC_LIBRARIES})
endif ()
