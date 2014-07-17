# Try to find libcaf headers and library.
#
# Use this module as follows:
#
#     find_package(Libcaf)
#
# Variables used by this module (they can change the default behaviour and need
# to be set before calling find_package):
#
#  LIBCAF_ROOT_DIR  Set this variable to the root installation of
#                   libcaf if the module has problems finding 
#                   the proper installation path.
#
# Variables defined by this module:
#
#  LIBCAF_FOUND              System has libcaf headers and library
#  LIBCAF_LIBRARIES          Location of the libcaf library
#  LIBCAF_INCLUDE_DIR        Location of the libcaf headers

find_path(LIBCAF_INCLUDE_DIR
    NAMES caf/all.hpp
    HINTS ${LIBCAF_ROOT_DIR}/include)

find_library(LIBCAF_LIBRARIES
    NAMES caf
    HINTS ${LIBCAF_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libcaf DEFAULT_MSG
    LIBCAF_LIBRARIES
    LIBCAF_INCLUDE_DIR)

mark_as_advanced(
    LIBCAF_ROOT_DIR
    LIBCAF_LIBRARIES
    LIBCAF_INCLUDE_DIR)
