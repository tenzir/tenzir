# Findlibunwind
# ---------
#
# Find the libunwind includes and library.
#
# Result Variables
# ^^^^^^^^^^^^^^^^
#
# This module will set the following variables in your project:
#
# ``libunwind_INCLUDE_DIRS``
#   where to find rpc.h, etc.
# ``libunwind_LIBRARIES``
#   the libraries to link against to use TIRPC.
# ``libunwind_VERSION``
#   the version of TIRPC found.
# ``libunwind_FOUND``
#   true if the TIRPC headers and libraries were found.
#

find_package(PkgConfig)
if (PkgConfig_FOUND)
  pkg_check_modules(PC_libunwind libunwind)

  find_path(
    libunwind_INCLUDE_DIRS
    NAMES libunwind.h
    HINTS ${PC_libunwind_INCLUDE_DIRS})

  find_library(
    libunwind_LIBRARIES
    NAMES unwind
    HINTS ${PC_libunwind_LIBRARY_DIRS})

  set(libunwind_VERSION ${PC_libunwind_VERSION})

  include(FindPackageHandleStandardArgs)

  find_package_handle_standard_args(
    libunwind
    REQUIRED_VARS libunwind_LIBRARIES libunwind_INCLUDE_DIRS
    VERSION_VAR libunwind_VERSION)

  mark_as_advanced(libunwind_INCLUDE_DIRS libunwind_LIBRARIES)

  if (libunwind_FOUND)
    # Some versions of libunwind have a private dependency on liblzma. If it's
    # available we link it as well.
    find_package(LibLZMA QUIET)
    if (LibLZMA_FOUND)
      list(PREPEND libunwind_LIBRARIES "LibLZMA::LibLZMA")
    endif ()

    if (NOT TARGET unwind::unwind)
      add_library(unwind::unwind INTERFACE IMPORTED GLOBAL)
      set_target_properties(
        unwind::unwind
        PROPERTIES IMPORTED_LOCATION "${libunwind_LIBRARIES}"
                   INTERFACE_INCLUDE_DIRECTORIES "${libunwind_INCLUDE_DIRS}"
                   INTERFACE_LINK_LIBRARIES "${libunwind_LIBRARIES}")
    endif ()
  endif ()
endif ()
