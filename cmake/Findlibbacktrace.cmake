# Findlibbacktrace
# -----------------
#
# Find the libbacktrace includes and library.
#
# Result Variables
# ^^^^^^^^^^^^^^^^
#
# ``libbacktrace_INCLUDE_DIRS``
#   where to find backtrace.h.
# ``libbacktrace_LIBRARIES``
#   the libraries to link against to use libbacktrace.
# ``libbacktrace_FOUND``
#   true if the headers and libraries were found.

if (libbacktrace_ROOT)
  message(STATUS "Using libbacktrace_ROOT: ${libbacktrace_ROOT}")
endif ()

find_path(
  libbacktrace_INCLUDE_DIRS
  NAMES backtrace.h
  PATHS ${libbacktrace_ROOT}
  PATH_SUFFIXES include)

find_library(
  libbacktrace_LIBRARIES
  NAMES backtrace libbacktrace
  PATHS ${libbacktrace_ROOT}
  PATH_SUFFIXES lib lib64)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  libbacktrace REQUIRED_VARS libbacktrace_LIBRARIES libbacktrace_INCLUDE_DIRS)

mark_as_advanced(libbacktrace_INCLUDE_DIRS libbacktrace_LIBRARIES)

if (libbacktrace_FOUND AND NOT TARGET libbacktrace::libbacktrace)
  add_library(libbacktrace::libbacktrace UNKNOWN IMPORTED GLOBAL)
  set_target_properties(
    libbacktrace::libbacktrace
    PROPERTIES IMPORTED_LOCATION "${libbacktrace_LIBRARIES}"
               INTERFACE_INCLUDE_DIRECTORIES "${libbacktrace_INCLUDE_DIRS}")
endif ()
