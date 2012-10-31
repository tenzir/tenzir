# Tries to find Broccoli headers and libraries
#
# Usage of this module as follows:
#
#     find_package(Broccoli)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Broccoli_ROOT_DIR  Set this variable to the root installation of
#                    Broccoli if the module has problems finding 
#                    the proper installation path.
#
# Variables defined by this module:
#
#  BROCCOLI_FOUND              System has Broccoli libs/headers
#  BROCCOLI_LIBRARIES          The Broccoli libraries
#  BROCCOLI_INCLUDE_DIR        The location of Broccoli headers

find_path(BROCCOLI_INCLUDE_DIR
  NAMES broccoli.h
  HINTS ${Broccoli_ROOT_DIR}/include)

find_library(BROCCOLI_LIBRARIES
  NAMES broccoli
  HINTS ${Broccoli_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  Broccoli
  DEFAULT_MSG
  BROCCOLI_LIBRARIES
  BROCCOLI_INCLUDE_DIR)

mark_as_advanced(
  BROCCOLI_ROOT_DIR
  BROCCOLI_LIBRARIES
  BROCCOLI_INCLUDE_DIR)
