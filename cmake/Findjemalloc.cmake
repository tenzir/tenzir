if (BUILD_SHARED_LIBS)
  set(search_target
      "${CMAKE_SHARED_LIBRARY_PREFIX}jemalloc${CMAKE_SHARED_LIBRARY_SUFFIX}")
else ()
  set(search_target
      "${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc${CMAKE_STATIC_LIBRARY_SUFFIX}")
endif ()

if (jemalloc_ROOT_DIR)
  message(STATUS "Using jemalloc_ROOT: ${jemalloc_ROOT_DIR}")
  find_path(
    jemalloc_INCLUDE_DIR
    NAMES jemalloc/jemalloc.h
    PATHS "${jemalloc_ROOT_DIR}/include"
    PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES}
    NO_DEFAULT_PATH)

  find_library(
    jemalloc_LIBRARY
    NAMES "${search_target}"
    PATHS "${jemalloc_ROOT_DIR}/lib"
    PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
    NO_DEFAULT_PATH)
else ()
  find_package(PkgConfig QUIET)
  pkg_check_modules(PC_jemalloc QUIET jemalloc)

  message("jemalloc dirs: ${PC_jemalloc_INCLUDE_DIRS}")

  find_path(
    jemalloc_INCLUDE_DIR
    NAMES jemalloc/jemalloc.h
    HINTS ${PC_jemalloc_INCLUDE_DIRS})

  if (BUILD_SHARED_LIBS)
    find_library(
      jemalloc_LIBRARY
      NAMES "${search_target}"
      HINTS ${PC_jemalloc_LIBRARY_DIRS})
  else ()
    find_library(
      jemalloc_LIBRARY
      NAMES "${search_target}"
      HINTS ${PC_jemalloc_STATIC_LIBRARY_DIRS})
  endif ()
endif ()

if (jemalloc_INCLUDE_DIR)
  set(_version_regex "^#define[ \t]+JEMALLOC_VERSION[ \t]+\"([^\"]+)\".*")
  file(STRINGS "${jemalloc_INCLUDE_DIR}/jemalloc/jemalloc.h" jemalloc_VERSION
       REGEX "${_version_regex}")
  string(REGEX REPLACE "${_version_regex}" "\\1" jemalloc_VERSION
                       "${jemalloc_VERSION}")
  unset(_version_regex)
endif ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  jemalloc
  REQUIRED_VARS jemalloc_LIBRARY jemalloc_INCLUDE_DIR
  VERSION_VAR jemalloc_VERSION)

if (jemalloc_FOUND)
  message(STATUS "Found jemalloc: ${jemalloc_LIBRARY}")
  set(jemalloc_LIBRARIES ${jemalloc_LIBRARY})
  set(jemalloc_INCLUDE_DIRS ${jemalloc_INCLUDE_DIR})
endif ()

mark_as_advanced(jemalloc_INCLUDE_DIR jemalloc_LIBRARY)

# We use this somewhat unusual target name to avoid a collision with
# the bundled jemalloc from Apache Arrow.
# TODO: Rename to jemalloc::jemalloc once
# https://issues.apache.org/jira/browse/ARROW-7605 is resolved.
if (jemalloc_FOUND AND NOT (TARGET jemalloc::jemalloc_))
  add_library(jemalloc::jemalloc_ UNKNOWN IMPORTED)
  set_target_properties(
    jemalloc::jemalloc_
    PROPERTIES IMPORTED_LOCATION ${jemalloc_LIBRARIES}
               INTERFACE_INCLUDE_DIRECTORIES ${jemalloc_INCLUDE_DIRS})
endif ()
