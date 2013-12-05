cmake_minimum_required(VERSION 2.6 FATAL_ERROR)
project(VAST CXX)

# Prohibit in-source builds.
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed. Please use "
    "./configure to choose a build directory and "
    "initialize the build configuration.")
endif ()

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(Helpers)

# Support for 'make uninstall'.
if ("${PROJECT_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
  configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
    @ONLY)

  add_custom_target(uninstall COMMAND
    ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
endif ()

set(CMAKE_VERBOSE_MAKEFILE false
  CACHE STRING "Show all outputs including compiler lines." FORCE)

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" VERSION LIMIT_COUNT 1)
string(REPLACE "." " " version_numbers ${VERSION})
separate_arguments(version_numbers)
list(GET version_numbers 0 VERSION_MAJOR)
list(GET version_numbers 1 VERSION_MINOR)
set(VERSION_MAJ_MIN "${VERSION_MAJOR}.${VERSION_MINOR}")

# We need -ftemplate-depth=256 to make Boost Spirit happy.
set(CMAKE_CXX_FLAGS                "-Wall -Wextra -std=c++11 -ftemplate-depth=256")
set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os")
set(CMAKE_CXX_FLAGS_RELEASE        "-O4")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")
# TODO: Fix variadic logging and then add -pedantic again.

if (ENABLE_DEBUG)
  set(CMAKE_BUILD_TYPE Debug)
  set(VAST_LOG_LEVEL 6)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DVAST_DEBUG")
endif ()
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()

if("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)
  if (NOT (GCC_VERSION VERSION_GREATER 4.8 OR GCC_VERSION VERSION_EQUAL 4.8))
    message(FATAL_ERROR "${PROJECT_NAME} requires g++ 4.8 or greater.")
  endif ()
elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
else()
  message(FATAL_ERROR "Your C++ compiler does not support C++11.")
endif ()

include(MacDependencyPaths)

if (BOOST_ROOT)
  # Prevent falling back to system paths when using a custom Boost prefix.
  set(Boost_NO_SYSTEM_PATHS true)
endif ()
find_package(Boost 1.50.0 COMPONENTS regex unit_test_framework)
if (Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
else ()
  message(FATAL_ERROR "Could not find Boost")
endif ()

find_package(Libcppa)
if (LIBCPPA_FOUND)
  include_directories(${LIBCPPA_INCLUDE_DIR})
else ()
  message(FATAL_ERROR "Could not find libcppa")
endif ()

find_package(Broccoli QUIET)
if (BROCCOLI_FOUND)
  set(VAST_HAVE_BROCCOLI true)
  include_directories(${BROCCOLI_INCLUDE_DIR})
endif ()

find_package(Editline QUIET)
if (EDITLINE_FOUND)
  set(VAST_HAVE_EDITLINE true)
  include_directories(${EDITLINE_INCLUDE_DIR})
endif ()

find_package(Gperftools QUIET)
if (GPERFTOOLS_FOUND)
  set(VAST_USE_PERFTOOLS_CPU_PROFILER true)
  include_directories(BEFORE ${GPERFTOOLS_INCLUDE_DIR})
endif ()
set(VAST_USE_PERFTOOLS_HEAP_PROFILER false)
if (ENABLE_PERFTOOLS_HEAP)
  set(VAST_USE_PERFTOOLS_HEAP_PROFILER true)
endif ()

find_package(Doxygen)
if (DOXYGEN_FOUND)
  add_subdirectory(doc)
endif ()

include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(aux)
add_subdirectory(src)
add_subdirectory(test)

include(ConfigurePackaging)
ConfigurePackaging(${VERSION})

string(TOUPPER ${CMAKE_BUILD_TYPE} build_type)

set(build_summary
    "\n====================|  Build Summary  |===================="
    "\n"
    "\nVersion:          ${VERSION_MAJ_MIN}"
    "\n"
    "\nSource directory: ${CMAKE_SOURCE_DIR}"
    "\nBuild directory:  ${CMAKE_BINARY_DIR}"
    "\nInstall prefix:   ${CMAKE_INSTALL_PREFIX}"
    "\nDebug mode:       ${ENABLE_DEBUG}"
    "\n"
    "\nBoost:            ${Boost_INCLUDE_DIR}"
    "\nLibcppa:          ${LIBCPPA_INCLUDE_DIR}"
    "\nBroccoli:         ${BROCCOLI_FOUND}"
    "\nEditline:         ${EDITLINE_FOUND}"
    "\nGperftools:       ${GPERFTOOLS_FOUND}"
    "\nUse tcmalloc:     ${VAST_USE_PERFTOOLS_HEAP_PROFILER}"
    "\n"
    "\nCXX:              ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:         ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${build_type}}")
if (BROCCOLI_FOUND)
  set(build_summary ${build_summary}
    "\nBroccoli:         ${BROCCOLI_INCLUDE_DIR}")
endif ()
set(build_summary ${build_summary}
    "\nDoxygen:          ${DOXYGEN_FOUND}"
    "\n"
    "\n===========================================================\n")

message(${build_summary})
