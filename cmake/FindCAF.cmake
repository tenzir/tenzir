# Try to find CAF headers and libraries.
#
# Use this module as follows:
#
# find_package(CAF [COMPONENTS <core|io|opencl|...>*] [REQUIRED])
#
# Variables used by this module (they can change the default behaviour and need
# to be set before calling find_package):
#
# CAF_ROOT_DIR  Set this variable either to an installation prefix or to a CAF build
# directory where to look for the CAF libraries.
#
# Variables defined by this module:
#
# CAF_FOUND              System has CAF headers and library
# CAF_LIBRARIES          List of library files for all components
# CAF_INCLUDE_DIRS       List of include paths for all components
# CAF_LIBRARY_$C         Library file for component $C
# CAF_INCLUDE_DIR_$C     Include path for component $C

if (CAF_FIND_COMPONENTS STREQUAL "")
  message(FATAL_ERROR "FindCAF requires at least one COMPONENT.")
endif ()

set(suffix "")
if (NOT BUILD_SHARED_LIBS)
  set(suffix "_static")
endif ()

find_package(Threads REQUIRED)

# iterate over user-defined components
foreach (comp ${CAF_FIND_COMPONENTS})
  # we use uppercase letters only for variable names
  string(TOUPPER "${comp}" UPPERCOMP)
  if ("${comp}" STREQUAL "core")
    set(HDRNAME "caf/all.hpp")
  elseif ("${comp}" STREQUAL "test")
    set(HDRNAME "caf/test/unit_test.hpp")
  else ()
    set(HDRNAME "caf/${comp}/all.hpp")
  endif ()
  if (CAF_ROOT_DIR)
    set(header_paths
        "${CAF_ROOT_DIR}/include" "${CAF_ROOT_DIR}/libcaf_${comp}"
        "${CAF_ROOT_DIR}/../libcaf_${comp}"
        "${CAF_ROOT_DIR}/../../libcaf_${comp}" NO_DEFAULT_PATH)
  else ()
    set(header_paths "/usr/include" "/usr/local/include" "/opt/local/include"
                     "/sw/include" "${CMAKE_INSTALL_PREFIX}/include")
  endif ()
  find_path(
    CAF_INCLUDE_DIR_${UPPERCOMP}
    NAMES ${HDRNAME}
    PATHS ${header_paths})
  mark_as_advanced(CAF_INCLUDE_DIR_${UPPERCOMP})
  if (NOT "${CAF_INCLUDE_DIR_${UPPERCOMP}}" STREQUAL
      "CAF_INCLUDE_DIR_${UPPERCOMP}-NOTFOUND")
    # mark as found (set back to false when missing library or build header)
    set(CAF_${comp}_FOUND true)
    # check for CMake-generated build header for the core component
    if ("${comp}" STREQUAL "core")
      # read content of config.hpp
      file(READ "${CAF_INCLUDE_DIR_CORE}/caf/config.hpp" CONFIG_HPP)
      # get line containing the version
      string(REGEX MATCH "#define CAF_VERSION [0-9]+" VERSION_LINE
                   "${CONFIG_HPP}")
      # extract version number from line
      string(REGEX MATCH "[0-9]+" VERSION_INT "${VERSION_LINE}")
      # calculate major, minor, and patch version
      math(EXPR CAF_VERSION_MAJOR "${VERSION_INT} / 10000")
      math(EXPR CAF_VERSION_MINOR "( ${VERSION_INT} / 100) % 100")
      math(EXPR CAF_VERSION_PATCH "${VERSION_INT} % 100")
      # create full version string
      set(CAF_VERSION
          "${CAF_VERSION_MAJOR}.${CAF_VERSION_MINOR}.${CAF_VERSION_PATCH}")
      find_path(
        caf_build_header_path
        NAMES caf/detail/build_config.hpp
        PATHS ${header_paths})
      if ("${caf_build_header_path}" STREQUAL "caf_build_header_path-NOTFOUND")
        message(WARNING "Found all.hpp for CAF core, but not build_config.hpp")
        set(CAF_${comp}_FOUND false)
      else ()
        if (NOT "${CAF_INCLUDE_DIR_${UPPERCOMP}}" STREQUAL
            "${caf_build_header_path}")
          list(APPEND CAF_INCLUDE_DIR_${UPPERCOMP} "${caf_build_header_path}")
        endif ()
      endif ()
    endif ()
    list(APPEND CAF_INCLUDE_DIRS "${CAF_INCLUDE_DIR_${UPPERCOMP}}")
    # look for (.dll|.so|.dylib) file, again giving hints for non-installed CAFs
    # skip probe_event as it is header only
    if (NOT ${comp} STREQUAL "probe_event" AND NOT ${comp} STREQUAL "test")
      if (CAF_ROOT_DIR)
        set(library_paths "${CAF_ROOT_DIR}/lib" NO_DEFAULT_PATH)
      else ()
        set(library_paths "/usr/lib" "/usr/local/lib" "/opt/local/lib"
                          "/sw/lib" "${CMAKE_INSTALL_PREFIX}/lib")
      endif ()
      find_library(
        CAF_LIBRARY_${UPPERCOMP}
        NAMES "caf_${comp}${suffix}"
        PATHS ${library_paths})
      mark_as_advanced(CAF_LIBRARY_${UPPERCOMP})
      if ("${CAF_LIBRARY_${UPPERCOMP}}" STREQUAL
          "CAF_LIBRARY_${UPPERCOMP}-NOTFOUND")
        set(CAF_${comp}_FOUND false)
      else ()
        set(CAF_LIBRARIES ${CAF_LIBRARIES} ${CAF_LIBRARY_${UPPERCOMP}})
      endif ()
    endif ()
  endif ()
endforeach ()

if (DEFINED CAF_INCLUDE_DIRS)
  list(REMOVE_DUPLICATES CAF_INCLUDE_DIRS)
endif ()

# let CMake check whether all requested components have been found
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  CAF
  REQUIRED_VARS CAF_LIBRARIES CAF_INCLUDE_DIRS
  VERSION_VAR CAF_VERSION
  HANDLE_COMPONENTS)

if (NOT CAF_FOUND)
  unset(CAF_LIBRARIES)
  unset(CAF_INCLUDE_DIRS)
endif ()

# final step to tell CMake we're done
mark_as_advanced(CAF_ROOT_DIR CAF_LIBRARIES CAF_INCLUDE_DIRS)

if (CAF_FOUND)
  if (CAF_core_FOUND AND NOT TARGET CAF::core)
    add_library(CAF::core UNKNOWN IMPORTED GLOBAL)
    set_target_properties(
      CAF::core
      PROPERTIES IMPORTED_LOCATION "${CAF_LIBRARY_CORE}"
                 INTERFACE_INCLUDE_DIRECTORIES "${CAF_INCLUDE_DIR_CORE}"
                 INTERFACE_LINK_LIBRARIES "Threads::Threads")
  endif ()
  if (CAF_io_FOUND AND NOT TARGET CAF::io)
    add_library(CAF::io UNKNOWN IMPORTED GLOBAL)
    set_target_properties(
      CAF::io
      PROPERTIES IMPORTED_LOCATION "${CAF_LIBRARY_IO}"
                 INTERFACE_INCLUDE_DIRECTORIES "${CAF_INCLUDE_DIR_IO}"
                 INTERFACE_LINK_LIBRARIES "CAF::core")
  endif ()
  if (CAF_openssl_FOUND AND NOT TARGET CAF::openssl)
    add_library(CAF::openssl UNKNOWN IMPORTED GLOBAL)
    set_target_properties(
      CAF::openssl
      PROPERTIES IMPORTED_LOCATION "${CAF_LIBRARY_OPENSSL}"
                 INTERFACE_INCLUDE_DIRECTORIES "${CAF_INCLUDE_DIR_OPENSSL}"
                 INTERFACE_LINK_LIBRARIES "CAF::core;CAF::io")
    if (NOT BUILD_SHARED_LIBS)
      include(CMakeFindDependencyMacro)
      set(OPENSSL_USE_STATIC_LIBS TRUE)
      find_dependency(OpenSSL)
      set_property(
        TARGET CAF::openssl
        APPEND
        PROPERTY INTERFACE_LINK_LIBRARIES "OpenSSL::SSL")
    endif ()
  endif ()
  if (CAF_opencl_FOUND AND NOT TARGET CAF::opencl)
    add_library(CAF::opencl UNKNOWN IMPORTED GLOBAL)
    set_target_properties(
      CAF::opencl
      PROPERTIES IMPORTED_LOCATION "${CAF_LIBRARY_OPENCL}"
                 INTERFACE_INCLUDE_DIRECTORIES "${CAF_INCLUDE_DIR_OPENCL}"
                 INTERFACE_LINK_LIBRARIES "CAF::core")
  endif ()
  if (CAF_test_FOUND AND NOT TARGET CAF::test)
    add_library(CAF::test INTERFACE IMPORTED GLOBAL)
    set_target_properties(
      CAF::test
      PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${CAF_INCLUDE_DIR_TEST}"
                 INTERFACE_LINK_LIBRARIES "CAF::core")
  endif ()
endif ()
