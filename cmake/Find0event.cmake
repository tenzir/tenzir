# Tries to find 0event headers and library.
#
# Use this module as follows:
#
#     find_package(0event)
#
# Variables used by this module (they can change the default behaviour and need
# to be set before calling find_package):
#
#  0event_ROOT_DIR  Set this variable to the root installation of
#                   0event if the module has problems finding 
#                   the proper installation path.
#
# Variables defined by this module:
#
#  0EVENT_FOUND              System has 0event headers
#  0EVENT_LIBRARIES          The location of 0event headers
#  0EVENT_INCLUDE_DIR        The location of 0event headers

find_path(0EVENT_INCLUDE_DIR
  NAMES ze.h
  HINTS ${0event_ROOT_DIR}/include)

find_library(0EVENT_LIBRARIES
  NAMES ze
  HINTS ${0event_ROOT_DIR}/lib)

find_library(LZ4_LIBRARY
  NAMES lz4
  HINTS ${0event_ROOT_DIR}/lib)

find_library(LZ4HC_LIBRARY
  NAMES lz4hc
  HINTS ${0event_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  0event DEFAULT_MSG
  0EVENT_LIBRARIES
  0EVENT_INCLUDE_DIR)

set(0EVENT_LIBRARIES
  ${0EVENT_LIBRARIES}
  ${LZ4_LIBRARY}
  ${LZ4HC_LIBRARY})

mark_as_advanced(
  0event_ROOT_DIR
  0EVENT_LIBRARIES
  0EVENT_INCLUDE_DIR)
