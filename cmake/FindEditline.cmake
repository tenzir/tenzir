# Tries to find editline headers and libraries
#
# Usage of this module as follows:
#
#     find_package(Editline)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  EDITLINE_ROOT_DIR  Set this variable to the root installation of
#                     editline if the module has problems finding
#                     the proper installation path.
#
# Variables defined by this module:
#
#  EDITLINE_FOUND              System has Editline libs/headers
#  EDITLINE_LIBRARIES          The Editline libraries
#  EDITLINE_INCLUDE_DIR        The location of Editline headers
#  EDITLINE_VERSION            The full version of Editline
#  EDITLINE_VERSION_MAJOR      The version major of Editline
#  EDITLINE_VERSION_MINOR      The version minor of Editline

find_path(EDITLINE_INCLUDE_DIR
  NAMES histedit.h
  HINTS ${EDITLINE_ROOT_DIR}/include)

if (EDITLINE_INCLUDE_DIR)
  file(STRINGS ${EDITLINE_INCLUDE_DIR}/histedit.h
    editline_header REGEX "^#define.LIBEDIT_[A-Z]+.*$")

  string(REGEX REPLACE ".*#define.LIBEDIT_MAJOR[ \t]+([0-9]+).*" "\\1"
    EDITLINE_VERSION_MAJOR ${editline_header})
  string(REGEX REPLACE ".*#define.LIBEDIT_MINOR[ \t]+([0-9]+).*" "\\1"
    EDITLINE_VERSION_MINOR ${editline_header})

  set(EDITLINE_VERSION_MAJOR ${EDITLINE_VERSION_MAJOR} CACHE STRING "" FORCE)
  set(EDITLINE_VERSION_MINOR ${EDITLINE_VERSION_MINOR} CACHE STRING "" FORCE)
  set(EDITLINE_VERSION ${EDITLINE_VERSION_MAJOR}.${EDITLINE_VERSION_MINOR}
    CACHE STRING "" FORCE)
endif ()

find_library(EDITLINE_LIBRARIES
  NAMES edit
  HINTS ${EDITLINE_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  Editline
  DEFAULT_MSG
  EDITLINE_LIBRARIES
  EDITLINE_INCLUDE_DIR)

mark_as_advanced(
  EDITLINE_ROOT_DIR
  EDITLINE_LIBRARIES
  EDITLINE_INCLUDE_DIR
  EDITLINE_VERSION
  EDITLINE_VERSION_MAJOR
  EDITLINE_VERSION_MINOR
  )
