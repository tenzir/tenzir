cmake_minimum_required(VERSION 3.1)
include(GNUInstallDirs)

project(tsl-robin-map VERSION 0.6.2)

add_library(robin_map INTERFACE)
# Use tsl::robin_map as target, more consistent with other libraries conventions (Boost, Qt, ...)
add_library(tsl::robin_map ALIAS robin_map)

target_include_directories(
  robin_map INTERFACE "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
                      "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>")

list(
  APPEND
  headers
  "${CMAKE_CURRENT_SOURCE_DIR}/include/tsl/robin_growth_policy.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/tsl/robin_hash.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/tsl/robin_map.h"
  "${CMAKE_CURRENT_SOURCE_DIR}/include/tsl/robin_set.h")
target_sources(robin_map INTERFACE "$<BUILD_INTERFACE:${headers}>")

if (MSVC)
  target_sources(
    robin_map
    INTERFACE
      "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/tsl-robin-map.natvis>"
      "$<INSTALL_INTERFACE:${CMAKE_INSTALL_DATAROOTDIR}/tsl-robin-map.natvis>")
endif ()

# Installation (only compatible with CMake version >= 3.3)
if (${CMAKE_VERSION} VERSION_GREATER "3.2")
  include(CMakePackageConfigHelpers)

  ## Install include directory and potential natvis file
  install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/tsl"
          DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

  if (MSVC)
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/tsl-robin-map.natvis"
            DESTINATION "${CMAKE_INSTALL_DATAROOTDIR}")
  endif ()

  ## Create and install tsl-robin-mapTargets.cmake
  install(TARGETS robin_map EXPORT tsl-robin-mapTargets)
endif ()
