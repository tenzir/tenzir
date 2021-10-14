# - Try to find xxhash
# Once done this will define the following variables:

#  XXHASH_FOUND - System has libxxhash
#  XXHASH_INCLUDE_DIRS - The libsystemd include directories
#  XXHASH_LIBRARIES - The libraries needed to use libsystemd

find_package(PkgConfig)

if (PkgConfig_FOUND)
  pkg_check_modules(PC_XXHASH QUIET libxxhash)

  find_library(XXHASH_LIBRARIES NAMES xxhash ${PC_XXHASH_LIBRARY_DIRS})
  find_path(XXHASH_INCLUDE_DIRS xxhash.h HINTS ${PC_XXHASH_INCLUDE_DIRS})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(xxhash DEFAULT_MSG XXHASH_INCLUDE_DIRS
                                    XXHASH_LIBRARIES)
  mark_as_advanced(XXHASH_FOUND XXHASH_INCLUDE_DIRS XXHASH_LIBRARIES)

  if (XXHASH_FOUND)
    message("xxhash: ${XXHASH_LIBRARIES}")
    if (NOT TARGET xxhash::xxhash)
      add_library(xxhash::xxhash UNKNOWN IMPORTED)
      set_target_properties(
        xxhash::xxhash
        PROPERTIES IMPORTED_LOCATION "${XXHASH_LIBRARIES}"
                   INTERFACE_INCLUDE_DIRECTORIES "${XXHASH_INCLUDE_DIRS}"
                   INTERFACE_LINK_LIBRARIES "${XXHASH_LIBRARIES}")
    endif ()
    if (NOT TARGET xxhash::xxhash_header_only)
      add_library(xxhash::xxhash_header_only UNKNOWN IMPORTED)
      set_target_properties(
        xxhash::xxhash_header_only
        PROPERTIES IMPORTED_LOCATION "${XXHASH_LIBRARIES}"
                   INTERFACE_INCLUDE_DIRECTORIES "${XXHASH_INCLUDE_DIRS}"
                   INTERFACE_COMPILE_DEFINITIONS XXH_INLINE_ALL=1)
    endif ()
  endif ()
endif ()
