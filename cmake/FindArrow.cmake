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
#  ARROW_FOUND              System has Arrow libs/headers
#  ARROW_LIBRARIES          The Arrow libraries
#  ARROW_INCLUDE_DIR        The location of Arrow headers

find_library(ARROW_LIBRARIES
  NAMES arrow
  HINTS
    ${ARROW_ROOT_DIR}
    ${ARROW_ROOT_DIR}/debug
    ${ARROW_ROOT_DIR}/release
    ${ARROW_ROOT_DIR}/relwithdebinfo
    ${ARROW_ROOT_DIR}/minsizerel
    ${ARROW_ROOT_DIR}/lib)

find_path(ARROW_INCLUDE_DIR
  NAMES arrow/api.h
  HINTS
    ${ARROW_ROOT_DIR}/../src
    ${ARROW_ROOT_DIR}/include)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  ARROW
  DEFAULT_MSG
  ARROW_LIBRARIES
  ARROW_INCLUDE_DIR)

mark_as_advanced(
  ARROW_ROOT_DIR
  ARROW_LIBRARIES
  ARROW_INCLUDE_DIR)
