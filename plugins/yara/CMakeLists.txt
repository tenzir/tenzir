cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  yara
  DESCRIPTION "YARA-X plugin for Tenzir"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside Tenzir, it is necessary to specify this
# line manually so plugins can be linked against an installed Tenzir.
include(CTest)

find_package(Tenzir REQUIRED)
TenzirRegisterPlugin(TARGET yara ENTRYPOINT src/plugin.cpp)

find_package(PkgConfig REQUIRED QUIET)
pkg_check_modules(yara_x_capi REQUIRED IMPORTED_TARGET "yara_x_capi>=1.19.0")
dependency_summary("YARA-X" PkgConfig::yara_x_capi "Dependencies")
target_link_libraries(yara PUBLIC PkgConfig::yara_x_capi)

# The static library needs the flags in the `.private` pkg-config section,
# which CMake does not expose through the imported target.
if (TENZIR_ENABLE_STATIC_EXECUTABLE)
  target_link_libraries(PkgConfig::yara_x_capi
                        INTERFACE ${yara_x_capi_STATIC_LIBRARIES})
endif ()
