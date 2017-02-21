# Tries to find the *gcov* executable.
#
# Usage of this module as follows:
#
#     find_package(GCOV)
#
# Variables defined by this module:
#
#  GCOV_FOUND              Flag indicating whether gcov was found.
#  GCOV                    The gcov executable.

find_program(GCOV
  NAMES gcov-6 gcov
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  Gcov
  DEFAULT_MSG
  GCOV)

mark_as_advanced(GCOV)
