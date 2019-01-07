# Tries to find Snappy.
#
# Usage of this module as follows:
#
#     find_package(SNAPPY)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  SNAPPY_ROOT_DIR  Set this variable to the root installation of
#                   Snappy if the module has problems finding
#                   the proper installation path.
#
# Variables defined by this module:
#
#  SNAPPY_FOUND              System has Snappy libs/headers
#  SNAPPY_LIBRARIES          The Snappy libraries (tcmalloc & profiler)
#  SNAPPY_INCLUDE_DIR        The location of Snappy headers

find_library(SNAPPY_LIBRARIES
  NAMES snappy
  HINTS ${Snappy_ROOT_DIR}/lib)

find_path(SNAPPY_INCLUDE_DIR
  NAMES snappy.h
  HINTS ${Snappy_ROOT_DIR}/include)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  SNAPPY
  DEFAULT_MSG
  SNAPPY_LIBRARIES
  SNAPPY_INCLUDE_DIR)

mark_as_advanced(
  SNAPPY_ROOT_DIR
  SNAPPY_LIBRARIES
  SNAPPY_INCLUDE_DIR)

# create IMPORTED target
if (SNAPPY_FOUND AND NOT TARGET snappy::snappy)
  add_library(snappy::snappy UNKNOWN IMPORTED)
  set_target_properties(snappy::snappy PROPERTIES
    IMPORTED_LOCATION ${SNAPPY_LIBRARIES})
endif()
