# Findlibmaxminddb
# ---------
#
# Find the libmaxminddb includes and library.
#
# Result Variables
# ^^^^^^^^^^^^^^^^
#
# This module will set the following variables in your project:
#
# ``libmaxminddb_INCLUDE_DIRS``
#   where to find maxminddb.h, etc.
# ``libmaxminddb_LIBRARIES``
#   the libraries to link against to use the MaxMindDB library.
# ``libmaxminddb_VERSION``
#   the version of the MaxMindDB library found.
# ``libmaxminddb_FOUND``
#   true if the MaxMindDB library was found.
#
find_package(PkgConfig)
if (PkgConfig_FOUND)
  pkg_check_modules(MDDB libmaxminddb)

  find_path(
    libmaxminddb_INCLUDE_DIRS
    NAMES maxminddb.h
    HINTS ${MDDB_INCLUDE_DIRS})

  find_library(
    libmaxminddb_LIBRARIES
    NAMES maxminddb
    HINTS ${MDDB_LIBRARIES})

  set(libmaxminddb_VERSION ${MDDB_VERSION})
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(
    libmaxminddb
    REQUIRED_VARS libmaxminddb_LIBRARIES libmaxminddb_INCLUDE_DIRS
    VERSION_VAR libmaxminddb_VERSION)

  mark_as_advanced(libmaxminddb_INCLUDE_DIRS libmaxminddb_LIBRARIES)

  if (libmaxminddb_FOUND)
    if (NOT TARGET maxminddb::maxminddb)
      add_library(maxminddb::maxminddb INTERFACE IMPORTED GLOBAL)
      set_target_properties(
        maxminddb::maxminddb
        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${libmaxminddb_INCLUDE_DIRS}"
                   INTERFACE_LINK_LIBRARIES "${libmaxminddb_LIBRARIES}")
    endif ()
  endif ()
endif ()

