# Try to find 0event headers and library.
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
#  0event_LIBRARIES          The location of 0event headers
#  0event_INCLUDE_DIR        The location of 0event headers

find_path(0event_INCLUDE_DIR
    NAMES ze.h
    HINTS ${0event_ROOT_DIR}/include)

find_library(0event_LIBRARIES
    NAMES ze
    HINTS ${0event_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(0event DEFAULT_MSG
    0event_LIBRARIES
    0event_INCLUDE_DIR)

mark_as_advanced(
    0event_ROOT_DIR
    0event_LIBRARIES
    0event_INCLUDE_DIR)
