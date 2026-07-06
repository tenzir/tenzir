cmake_minimum_required(VERSION 3.25...3.28 FATAL_ERROR)

project(
  iceberg
  DESCRIPTION "Apache Iceberg integration for Tenzir"
  LANGUAGES CXX)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET iceberg
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB src/*.cpp
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp"
  INCLUDE_DIRECTORIES include)

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
  add_subdirectory(aux/iceberg-cpp)
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
  iceberg_relax_warnings("${CMAKE_CURRENT_SOURCE_DIR}/aux/iceberg-cpp")

  foreach (_target IN ITEMS iceberg_static iceberg_data_static
                            iceberg_bundle_static iceberg_rest_static)
    if (TARGET "${_target}")
      TenzirSystemizeTarget("${_target}")
    endif ()
  endforeach ()

  target_link_libraries(iceberg PUBLIC iceberg_bundle_static
                                       iceberg_rest_static)

  dependency_summary(
    "iceberg-cpp" "${CMAKE_CURRENT_SOURCE_DIR}/aux/iceberg-cpp" "Dependencies")
endif ()

option(TENZIR_ICEBERG_ENABLE_SPIKE
       "Build the throwaway Phase 0 iceberg spike executable" OFF)
if (TENZIR_ICEBERG_ENABLE_SPIKE)
  add_executable(iceberg-spike spike/spike.cpp src/facade.cpp)
  target_compile_features(iceberg-spike PRIVATE cxx_std_23)
  target_include_directories(iceberg-spike
                             PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include")
  target_link_libraries(iceberg-spike PRIVATE tenzir::libtenzir)
  if (Iceberg_FOUND)
    target_link_libraries(iceberg-spike PRIVATE iceberg::iceberg_bundle_static
                                                iceberg::iceberg_rest_static)
  else ()
    target_link_libraries(iceberg-spike PRIVATE iceberg_bundle_static
                                                iceberg_rest_static)
  endif ()
endif ()
