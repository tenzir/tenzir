# - Try to find FastBit headers and libraries
#
# Usage of this module as follows:
#
#     find_package(FastBit)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  FastBit_ROOT_DIR  Set this variable to the root installation of
#                    FastBit if the module has problems finding 
#                    the proper installation path.
#
# Variables defined by this module:
#
#  FASTBIT_FOUND              System has FastBit libs/headers
#  FastBit_LIBRARIES          The FastBit libraries
#  FastBit_INCLUDE_DIR        The location of FastBit headers

find_path(FastBit_INCLUDE_DIR
    NAMES ibis.h
    HINTS ${FastBit_ROOT_DIR}/include
)

find_library(FastBit_LIBRARIES
    NAMES fastbit
    HINTS ${FastBit_ROOT_DIR}/lib
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(FastBit DEFAULT_MSG
    FastBit_LIBRARIES
    FastBit_INCLUDE_DIR
)

mark_as_advanced(
    FastBit_ROOT_DIR
    FastBit_LIBRARIES
    FastBit_INCLUDE_DIR
)
