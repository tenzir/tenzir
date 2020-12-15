# Try to find xxhash headers and libraries.
#
# Use this module as follows:
#
# find_package(xxhash [REQUIRED])
#
# Variables defined by this module:
#
#   xxhash_FOUND          - True if xxhash is found.
#   xxhash_INCLUDE_DIRS   - Where to find xxhash.h
#   xxhash_VERSION        - The version of xxhash found (x.y.z)
#   xxhash_VERSION_MAJOR  - The major version of xxhash
#   xxhash_VERSION_MINOR  - The minor version of xxhash
#   xxhash_VERSION_PATCH  - The patch version of xxhash

find_path(
  xxhash_INCLUDE_DIR
  NAMES xxhash.h
  PATH_SUFFIXES include include/xxhash
  HINTS "${xxhash_ROOT}")

find_library(
  xxhash_LIBRARY
  NAMES xxhash
  PATH_SUFFIXES lib
  HINTS "${xxhash_ROOT}")

mark_as_advanced(xxhash_INCLUDE_DIR)

if (xxhash_INCLUDE_DIR AND EXISTS "${xxhash_INCLUDE_DIR}/xxhash.h")
  file(STRINGS "${xxhash_INCLUDE_DIR}/xxhash.h" XXHASH_H
       REGEX "^#define XXH_VERSION_[A-Z]+[ ]+[0-9]+$")
  string(REGEX REPLACE ".+XXH_VERSION_MAJOR[ ]+([0-9]+).*$" "\\1"
                       xxhash_VERSION_MAJOR "${XXHASH_H}")
  string(REGEX REPLACE ".+XXH_VERSION_MINOR[ ]+([0-9]+).*$" "\\1"
                       xxhash_VERSION_MINOR "${XXHASH_H}")
  string(REGEX REPLACE ".+XXH_VERSION_RELEASE[ ]+([0-9]+).*$" "\\1"
                       xxhash_VERSION_PATCH "${XXHASH_H}")
  set(xxhash_VERSION
      "${xxhash_VERSION_MAJOR}.${xxhash_VERSION_MINOR}.${xxhash_VERSION_PATCH}")
endif ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  xxhash
  REQUIRED_VARS xxhash_LIBRARY xxhash_INCLUDE_DIR
  VERSION_VAR xxhash_VERSION)

if (xxhash_FOUND)
  set(xxhash_INCLUDE_DIRS "${xxhash_INCLUDE_DIR}")

  if (NOT xxhash_LIBRARIES)
    set(xxhash_LIBRARIES ${xxhash_LIBRARY})
  endif ()

  if (NOT TARGET xxhash::xxhash)
    add_library(xxhash::xxhash UNKNOWN IMPORTED)
    set_target_properties(
      xxhash::xxhash
      PROPERTIES IMPORTED_LOCATION "${xxhash_LIBRARY}"
                 INTERFACE_INCLUDE_DIRECTORIES "${xxhash_INCLUDE_DIRS}")
  endif ()
endif ()
