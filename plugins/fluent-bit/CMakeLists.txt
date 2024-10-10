cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  fluent-bit
  DESCRIPTION "fluent-bit plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

file(GLOB_RECURSE fluentbit_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

TenzirRegisterPlugin(
  TARGET fluent-bit
  ENTRYPOINT src/plugin.cpp
  SOURCES ${fluentbit_sources}
  INCLUDE_DIRECTORIES include
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp")

find_package(fluentbit QUIET)
if (NOT fluentbit_FOUND)
  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
  find_package(fluentbit QUIET REQUIRED)
  list(POP_BACK CMAKE_MODULE_PATH)
endif ()

get_filename_component(FLUENT_BIT_PREFIX ${FLUENT_BIT_LOCATION} PATH)
dependency_summary("fluent-bit" ${FLUENT_BIT_PREFIX} "Dependencies")
target_link_libraries(fluent-bit PRIVATE fluentbit::fluentbit)

option(
  TENZIR_ENABLE_FLUENT_BIT_SO_WORKAROUNDS
  "Apply dynamic linking workarounds for quirky libfluent-bit.so builds on Linux"
  ON)
add_feature_info(
  "TENZIR_ENABLE_FLUENT_BIT_SO_WORKAROUNDS"
  TENZIR_ENABLE_FLUENT_BIT_SO_WORKAROUNDS
  "apply dynamic linking workarounds for quirky libfluent-bit.so builds on Linux."
)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux"
    AND NOT TENZIR_ENABLE_STATIC_PLUGINS
    AND TENZIR_ENABLE_FLUENT_BIT_SO_WORKAROUNDS)
  # The official packages install the shared library into the nonstandard
  # directory `/usr/lib/fluent-bit/`. CMake strips build rpaths during
  # installation, causing a failure during program loading.
  # We disable that behavior in the least intrusive way here.
  set_property(
    TARGET fluent-bit-shared
    APPEND
    PROPERTY INSTALL_RPATH "${FLUENT_BIT_LOCATION}")

  # The following condition implies `NOT TENZIR_ENABLE_STATIC_PLUGINS` but we
  # keep that for clarity.
  if ("${CMAKE_PROJECT_NAME}" STREQUAL "${PROJECT_NAME}")
    message(
      WARNING
        "The official builds of the fluent-bit library can't be loaded with"
        " dlopen() on Linux. You might have to add `${FLUENT_BIT_LIB}' to the"
        " LD_PRELOAD environment variable to run `tenzir` with the plugin"
        " enabled.")
  else ()
    # We can work around the problem above when the plugin is built together
    # with tenzir. The trick is to inject libfluent-bit.so into the main
    # dynamic link sequence of the `tenzir` binary.
    target_link_libraries(tenzir PRIVATE "${FLUENT_BIT_LIB}")
    # The rpath needs to be extended with the correct location just like above.
    set_property(
      TARGET tenzir
      APPEND
      PROPERTY INSTALL_RPATH "${FLUENT_BIT_LOCATION}")
  endif ()
endif ()
