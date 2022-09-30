# - Try to find http-parser
# Once done this will define the following variables:

#  HTTP_PARSER_FOUND - System has http_parser
#  HTTP_PARSER_INCLUDE_DIRS - The http_parser include directories.
#  HTTP_PARSER_LIBRARY - The http_parser library.

if (HTTP_PARSER_INCLUDE_DIR AND HTTP_PARSER_LIBRARY)
  # in cache already
  set(HTTP_PARSER_FIND_QUIETLY TRUE)
endif ()

find_path(HTTP_PARSER_INCLUDE_DIR http_parser.h
          HINTS ${HTTP_PARSER_DIR}/include)

find_library(
  HTTP_PARSER_LIBRARY
  NAMES http_parser
  HINTS ${HTTP_PARSER_DIR}/lib ${HTTP_PARSER_DIR}/bin)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(http-parser DEFAULT_MSG HTTP_PARSER_LIBRARY
                                  HTTP_PARSER_INCLUDE_DIR)

if (HTTP_PARSER_FOUND)
  set(HTTP_PARSER_LIBRARIES ${HTTP_PARSER_LIBRARY})
  set(HTTP_PARSER_INCLUDE_DIRS ${HTTP_PARSER_INCLUDE_DIR})
  if (NOT TARGET http-parser::http-parser)
    add_library(http-parser::http-parser UNKNOWN IMPORTED)
    set_target_properties(
      http-parser::http-parser
      PROPERTIES IMPORTED_LOCATION "${HTTP_PARSER_LIBRARY}"
                 INTERFACE_INCLUDE_DIRECTORIES "${HTTP_PARSER_INCLUDE_DIRS}"
                 INTERFACE_LINK_LIBRARIES "${HTTP_PARSER_LIBRARY}")

  endif ()
endif ()

mark_as_advanced(HTTP_PARSER_INCLUDE_DIR HTTP_PARSER_LIBRARY)
