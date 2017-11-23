# Tries to find Apache Arrow.
#
# Usage:
#
#     find_package(ARROW)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  ARROW_ROOT_DIR  Set this variable to the root installation of
#                  Arrow if the module has problems finding
#                  the proper installation path. If Arrow wasn't install
#                  system-wide, set this variable to the local build dir.
#
# Variables defined by this module:
#
#  ARROW_FOUND              System has (at least) Arrow C++ libs/headers
#  ARROW_CORE_LIBRARIES     The Arrow C++ libraries
#  ARROW_CORE_INCLUDE_DIR   The location of Arrow C++ headers
#  ARROW_PLASMA_FOUND       System has Plasma libs/headers
#  ARROW_PLASMA_LIBRARIES   The Plasma libraries
#  ARROW_PLASMA_INCLUDE_DIR The location of Plasma headers
#  ARROW_PLASMA_EXECUTABLE  The Plasma store executable

foreach (comp ${ARROW_FIND_COMPONENTS})
  string(TOUPPER "${comp}" UPPERCOMP)
  # Arrow puts all build files in a directory named as the build type.
  set(library_hints_for_build_directory
    ${ARROW_ROOT_DIR}/debug
    ${ARROW_ROOT_DIR}/release
    ${ARROW_ROOT_DIR}/relwithdebinfo
    ${ARROW_ROOT_DIR}/minsizerel)
  set(header_hints_for_build_directory
    ${ARROW_ROOT_DIR}/../src)
  if ("${comp}" STREQUAL "core")
    find_library(ARROW_CORE_LIBRARIES
      NAMES arrow
      HINTS
        ${ARROW_ROOT_DIR}/lib
        ${library_hints_for_build_directory})
    find_path(ARROW_CORE_INCLUDE_DIR
      NAMES arrow/api.h
      HINTS
        ${ARROW_ROOT_DIR}/include
        ${header_hints_for_build_directory})
    if (ARROW_CORE_LIBRARIES AND ARROW_CORE_INCLUDE_DIR)
      set(ARROW_CORE_FOUND true)
    endif ()
  elseif ("${comp}" STREQUAL "plasma")
    find_library(ARROW_PLASMA_LIBRARIES
      NAMES plasma
      HINTS
        ${ARROW_ROOT_DIR}
        ${ARROW_ROOT_DIR}/lib
        ${library_hints_for_build_directory})
    find_path(ARROW_PLASMA_INCLUDE_DIR
      NAMES plasma/client.h
      HINTS
        ${ARROW_ROOT_DIR}/include
        ${header_hints_for_build_directory})
    find_program(ARROW_PLASMA_EXECUTABLE
      plasma_store
      HINTS
        ${ARROW_ROOT_DIR}/bin
        ${library_hints_for_build_directory})
    if (ARROW_PLASMA_LIBRARIES AND ARROW_PLASMA_INCLUDE_DIR AND
        ARROW_PLASMA_EXECUTABLE)
      set(ARROW_PLASMA_FOUND true)
    endif ()
  else ()
    MESSAGE(FATAL_ERROR "invalid component: ${comp}")
  endif ()
endforeach ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  ARROW
  REQUIRED_VARS ARROW_CORE_FOUND
  HANDLE_COMPONENTS)

mark_as_advanced(
  ARROW_ROOT_DIR
  ARROW_CORE_LIBRARIES
  ARROW_CORE_INCLUDE_DIR
  ARROW_PLASMA_LIBRARIES
  ARROW_PLASMA_INCLUDE_DIR
  ARROW_PLASMA_EXECUTABLE)
