# Try to find libcppa headers and library.
#
# Use this module as follows:
#
#     find_package(libcppa)
#
# Variables used by this module (they can change the default behaviour and need
# to be set before calling find_package):
#
#  LIBCPPA_ROOT_DIR  Set this variable to the root installation of
#                    libcppa if the module has problems finding 
#                    the proper installation path.
#
# Variables defined by this module:
#
#  LIBCPPA_FOUND              System has libcppa headers and library
#  LIBCPPA_LIBRARIES          Location of the libcppa library
#  LIBCPPA_INCLUDE_DIR        Location of the libcppa headers

find_path(LIBCPPA_INCLUDE_DIR
    NAMES cppa/cppa.hpp
    HINTS ${LIBCPPA_ROOT_DIR}/include)

find_library(LIBCPPA_LIBRARIES
    NAMES cppa
    HINTS ${LIBCPPA_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libcppa DEFAULT_MSG
    LIBCPPA_LIBRARIES
    LIBCPPA_INCLUDE_DIR)

mark_as_advanced(
    LIBCPPA_ROOT_DIR
    LIBCPPA_LIBRARIES
    LIBCPPA_INCLUDE_DIR)
