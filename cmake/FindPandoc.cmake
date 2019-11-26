# Tries to find the *pandoc* executable
#
# Usage of this module as follows:
#
#     find_package(Pandoc)
#
# Variables defined by this module:
#
#  PANDOC_FOUND         Flag indicating whether pandoc was found.
#  PANDOC               The pandoc executable.

find_program(PANDOC
  NAMES pandoc
  DOC "pandoc man page generation tool"
)

if (PANDOC)
  set(PANDOC_FOUND true)
else ()
  set(PANDOC_FOUND false)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  Pandoc
  DEFAULT_MSG
  PANDOC)

mark_as_advanced(PANDOC)
