# Try to find c-ares.

find_package(PkgConfig QUIET)
if (PKG_CONFIG_FOUND)
  pkg_check_modules(PC_CARES QUIET libcares)
endif ()

find_path(
  CARES_INCLUDE_DIR
  NAMES ares.h
  HINTS ${PC_CARES_INCLUDEDIR})

find_library(
  CARES_LIBRARY
  NAMES cares
  HINTS ${PC_CARES_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  c-ares
  REQUIRED_VARS CARES_LIBRARY CARES_INCLUDE_DIR
  VERSION_VAR PC_CARES_VERSION)

if (c-ares_FOUND)
  if (NOT TARGET c-ares::cares)
    add_library(c-ares::cares UNKNOWN IMPORTED GLOBAL)
    set_target_properties(
      c-ares::cares
      PROPERTIES IMPORTED_LOCATION "${CARES_LIBRARY}"
                 INTERFACE_INCLUDE_DIRECTORIES "${CARES_INCLUDE_DIR}")
  endif ()
endif ()

mark_as_advanced(CARES_INCLUDE_DIR CARES_LIBRARY)
