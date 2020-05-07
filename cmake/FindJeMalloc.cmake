find_package(PkgConfig QUIET)
pkg_check_modules(PC_JeMalloc QUIET jemalloc)

message("jemalloc dirs: ${PC_JeMalloc_INCLUDE_DIRS}")

find_path(JeMalloc_INCLUDE_DIR
  NAMES jemalloc/jemalloc.h
  HINTS ${PC_JeMalloc_INCLUDE_DIRS}
)

if (BUILD_SHARED_LIBS)
find_library(JeMalloc_LIBRARY
  NAMES ${CMAKE_SHARED_LIBRARY_PREFIX}jemalloc${CMAKE_SHARED_LIBRARY_SUFFIX}
  HINTS ${PC_JeMalloc_LIBRARY_DIRS}
)
else ()
find_library(JeMalloc_LIBRARY
  NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc${CMAKE_STATIC_LIBRARY_SUFFIX}
  HINTS ${PC_JeMalloc_STATIC_LIBRARY_DIRS}
)
endif ()

if(JeMalloc_INCLUDE_DIR)
  set(_version_regex "^#define[ \t]+JEMALLOC_VERSION[ \t]+\"([^\"]+)\".*")
  file(STRINGS "${JeMalloc_INCLUDE_DIR}/jemalloc/jemalloc.h"
    JeMalloc_VERSION REGEX "${_version_regex}")
  string(REGEX REPLACE "${_version_regex}" "\\1"
    JeMalloc_VERSION "${JeMalloc_VERSION}")
  unset(_version_regex)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JeMalloc REQUIRED_VARS
  JeMalloc_LIBRARY JeMalloc_INCLUDE_DIR
  VERSION_VAR JeMalloc_VERSION)

if(JeMalloc_FOUND)
  message(STATUS "Found jemalloc: ${JeMalloc_LIBRARY}")
  set(JeMalloc_LIBRARIES    ${JeMalloc_LIBRARY})
  set(JeMalloc_INCLUDE_DIRS ${JeMalloc_INCLUDE_DIR})
endif()

mark_as_advanced(JeMalloc_INCLUDE_DIR JeMalloc_LIBRARY)

if(JeMalloc_FOUND AND NOT (TARGET JeMalloc::JeMalloc))
  add_library (JeMalloc::JeMalloc UNKNOWN IMPORTED)
  set_target_properties(JeMalloc::JeMalloc
    PROPERTIES
      IMPORTED_LOCATION ${JeMalloc_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${JeMalloc_INCLUDE_DIRS})
endif()
