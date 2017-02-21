# Tries to find the *gcovr* executable.
#
# Usage of this module as follows:
#
#     find_package(GCOVR)
#
# Variables defined by this module:
#
#  GCOVR_FOUND             Flag indicating whether gcovr was found.
#  GCOVR                   The gcovr executable.

find_program(GCOVR
  NAMES gcovr
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  GCOVR
  DEFAULT_MSG
  GCOVR)

mark_as_advanced(GCOVR)
