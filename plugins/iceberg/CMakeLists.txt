cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  iceberg
  DESCRIPTION "Apache Iceberg integration for Tenzir"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside Tenzir, it is necessary to specify this
# line manually so plugins can be linked against an installed Tenzir.
include(CTest)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET iceberg
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB src/*.cpp
  TEST_SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp"
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp"
  INCLUDE_DIRECTORIES include)

# The plugin statically bundles third-party code (iceberg-cpp and its
# vendored avro, cpr, croaring, and nanoarrow — and, depending on the
# packaging, static Arrow libraries). Keep those symbols out of the dynamic
# symbol table: plugins load with RTLD_GLOBAL, so an exported global that
# also exists in a shared library (e.g. libparquet's thrift-generated enum
# tables) gets unified by the dynamic linker, and both images then run
# constructors and exit-time destructors on the same object — a double free
# at shutdown. The tenzir_plugin_* entrypoints are compiled directly into
# the shared target and stay exported. ld64 on macOS has no equivalent, but
# its two-level namespace does not unify symbols across images anyway.
if (TARGET iceberg-shared AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(iceberg-shared PRIVATE "LINKER:--exclude-libs,ALL")
endif ()

# Google Cloud support is optional: catalog authentication mints OAuth2
# tokens through google-cloud-cpp, and the data plane for gs:// table
# locations uses arrow's GcsFileSystem. Homebrew's apache-arrow ships with
# ARROW_GCS=OFF and homebrew-core carries no google-cloud-cpp, so builds
# without the prerequisites compile the plugin without Google Cloud support
# and reject the `gcp_*` arguments at pipeline validation instead.
find_package(Arrow REQUIRED CONFIG)
find_package(google_cloud_cpp_storage QUIET CONFIG)
if (ARROW_GCS AND google_cloud_cpp_storage_FOUND)
  dependency_summary("google-cloud-cpp" google-cloud-cpp::storage
                     "Dependencies")
  target_link_libraries(iceberg PUBLIC google-cloud-cpp::storage)
  target_compile_definitions(iceberg PUBLIC TENZIR_ICEBERG_GCS)
else ()
  message(
    STATUS
      "Disabling Google Cloud support for the iceberg plugin (requires Arrow"
      " with ARROW_GCS=ON and google-cloud-cpp)")
endif ()

option(TENZIR_ENABLE_BUNDLED_ICEBERG_CPP "Always use the bundled iceberg-cpp"
       "${TENZIR_ENABLE_BUNDLED_DEPENDENCIES}")
if (NOT TENZIR_ENABLE_BUNDLED_ICEBERG_CPP)
  find_package(Iceberg QUIET CONFIG)
endif ()

if (Iceberg_FOUND)
  dependency_summary("iceberg-cpp" iceberg::iceberg_bundle_static
                     "Dependencies")
  target_link_libraries(iceberg PUBLIC iceberg::iceberg_bundle_static
                                       iceberg::iceberg_rest_static)
else ()
  find_program(PATCH_EXECUTABLE patch REQUIRED)
  set(_iceberg_cpp_source "${CMAKE_CURRENT_BINARY_DIR}/aux/patched-iceberg-cpp")
  file(REMOVE_RECURSE "${_iceberg_cpp_source}")
  file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/aux/iceberg-cpp/"
       DESTINATION "${_iceberg_cpp_source}")
  execute_process(
    COMMAND "${PATCH_EXECUTABLE}" -p1 -i
            "${CMAKE_CURRENT_SOURCE_DIR}/aux/iceberg-cpp.patch"
    WORKING_DIRECTORY "${_iceberg_cpp_source}"
    RESULT_VARIABLE _iceberg_patch_result)
  if (NOT _iceberg_patch_result EQUAL 0)
    message(FATAL_ERROR "failed to patch bundled iceberg-cpp")
  endif ()

  set(ICEBERG_BUILD_STATIC
      ON
      CACHE BOOL "")
  set(ICEBERG_BUILD_SHARED
      OFF
      CACHE BOOL "")
  set(ICEBERG_BUILD_TESTS
      OFF
      CACHE BOOL "")
  set(ICEBERG_BUILD_BUNDLE
      ON
      CACHE BOOL "")
  set(ICEBERG_BUILD_REST
      ON
      CACHE BOOL "")
  set(ICEBERG_S3
      ON
      CACHE BOOL "")
  set(ICEBERG_SIGV4
      ON
      CACHE BOOL "")

  set(_build_shared_libs_before "${BUILD_SHARED_LIBS}")
  set(BUILD_SHARED_LIBS OFF)
  set(_pic_before "${CMAKE_POSITION_INDEPENDENT_CODE}")
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
  add_subdirectory("${_iceberg_cpp_source}"
                   "${CMAKE_CURRENT_BINARY_DIR}/aux/iceberg-cpp")
  set(BUILD_SHARED_LIBS "${_build_shared_libs_before}")
  unset(_build_shared_libs_before)
  set(CMAKE_POSITION_INDEPENDENT_CODE "${_pic_before}")
  unset(_pic_before)

  # iceberg-cpp generates version.h into ${CMAKE_BINARY_DIR}/src/iceberg, which
  # is only correct when it is the top-level project; its include paths expect
  # the file relative to its own binary dir.
  file(COPY_FILE "${CMAKE_BINARY_DIR}/src/iceberg/version.h"
       "${CMAKE_CURRENT_BINARY_DIR}/aux/iceberg-cpp/src/iceberg/version.h"
       ONLY_IF_DIFFERENT)

  # iceberg-cpp enables CMAKE_COMPILE_WARNING_AS_ERROR for its own targets; we
  # do not want a vendored dependency to break our build on new compiler
  # warnings.
  function (iceberg_relax_warnings directory)
    get_property(
      _subdirs
      DIRECTORY "${directory}"
      PROPERTY SUBDIRECTORIES)
    foreach (_subdir IN LISTS _subdirs)
      iceberg_relax_warnings("${_subdir}")
    endforeach ()
    get_property(
      _targets
      DIRECTORY "${directory}"
      PROPERTY BUILDSYSTEM_TARGETS)
    foreach (_target IN LISTS _targets)
      get_target_property(_type "${_target}" TYPE)
      if (NOT _type STREQUAL "INTERFACE_LIBRARY")
        set_target_properties("${_target}" PROPERTIES COMPILE_WARNING_AS_ERROR
                                                      OFF)
      endif ()
    endforeach ()
  endfunction ()
  iceberg_relax_warnings("${_iceberg_cpp_source}")

  foreach (_target IN ITEMS iceberg_static iceberg_data_static
                            iceberg_bundle_static iceberg_rest_static)
    if (TARGET "${_target}")
      TenzirSystemizeTarget("${_target}")
    endif ()
  endforeach ()

  target_link_libraries(iceberg PUBLIC iceberg_bundle_static
                                       iceberg_rest_static)

  dependency_summary("iceberg-cpp" "${_iceberg_cpp_source}" "Dependencies")
endif ()
