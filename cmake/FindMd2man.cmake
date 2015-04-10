# Tries to find the *md2man* executable
#
# Usage of this module as follows:
#
#     find_package(Md2man)
#
# Variables defined by this module:
#
#  MD2MAN_FOUND              Flag indicating whether md2man was found.
#  MD2MAN_ROFF               The md2man-roff executable.

find_program(MD2MAN_ROFF
  NAMES md2man-roff
  DOC "md2man man page generation tool"
)

if (MD2MAN_ROFF)
  set(MD2MAN_FOUND true)
else ()
  set(MD2MAN_FOUND false)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  Md2man
  DEFAULT_MSG
  MD2MAN_ROFF)

mark_as_advanced(MD2MAN_ROFF)
