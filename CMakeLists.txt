cmake_minimum_required(VERSION 2.6 FATAL_ERROR)
project(VAST CXX)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(CTest)
include(Helpers)
include(MacDependencyPaths)
include(ConfigurePackaging)

# Prohibit in-source builds.
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed. Please use "
    "./configure to choose a build directory and "
    "initialize the build configuration.")
endif ()

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

if (NOT CMAKE_CXX_FLAGS)
  # TODO: Fix variadic logging macros and then add -pedantic again.
  # We need -ftemplate-depth=256 to make Boost Spirit happy.
  set(CXX_FLAGS                      "-Wall -Wextra -std=c++1y -ftemplate-depth=256")
  set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
  set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os")
  set(CMAKE_CXX_FLAGS_RELEASE        "-O3")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")

  if (ENABLE_DEBUG)
    set(CMAKE_BUILD_TYPE Debug)
    set(CXX_FLAGS "${CXX_FLAGS} -DVAST_DEBUG")
  endif ()

  if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
  endif ()
endif()

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)
  if (NOT (GCC_VERSION VERSION_GREATER 4.8 OR GCC_VERSION VERSION_EQUAL 4.8))
    message(FATAL_ERROR "${PROJECT_NAME} requires g++ 4.8 or greater.")
  endif ()
  set(CXX_FLAGS "${CXX_FLAGS} -fdiagnostics-color=always")
elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  set(CXX_FLAGS "${CXX_FLAGS} -stdlib=libc++")
  if (ENABLE_ADDRESS_SANITIZER)
    set(CXX_FLAGS "${CXX_FLAGS} -fsanitize=address -g")
  endif ()
else ()
  message(FATAL_ERROR "Your C++ compiler does not support C++11.")
endif ()

# Only set our flags if the environment variable CXXFLAGS is not present.
if (NOT CMAKE_CXX_FLAGS)
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS})
endif ()

if (BOOST_ROOT)
  set(Boost_NO_SYSTEM_PATHS true)
elseif (VAST_PREFIX)
  set(BOOST_ROOT ${VAST_PREFIX})
  set(Boost_NO_SYSTEM_PATHS true)
endif ()
find_package(Boost 1.50.0)
if (Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
else ()
  message(FATAL_ERROR "Could not find Boost")
endif ()

if (NOT LIBCAF_ROOT_DIR AND VAST_PREFIX)
  set(LIBCAF_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(Libcaf COMPONENTS core io)
if (LIBCAF_FOUND)
  include_directories(${LIBCAF_INCLUDE_DIRS})
else ()
  message(FATAL_ERROR "Could not find libcaf")
endif ()

if (NOT PCAP_ROOT_DIR AND VAST_PREFIX)
  set(PCAP_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(PCAP QUIET)
if (PCAP_FOUND)
  set(VAST_HAVE_PCAP true)
  include_directories(${PCAP_INCLUDE_DIR})
endif ()

# FIXME: The Broccoli subsystem is currently defunct and needs a major
# overhaul. We'll temporarily disable Broccoli support entirely until this has
# been fixed.
set(BROCCOLI_FOUND false)
set(BROCCOLI_INCLUDE_DIR false)
#if (NOT BROCCOLI_ROOT_DIR AND VAST_PREFIX)
#  set(BROCCOLI_ROOT_DIR ${VAST_PREFIX})
#endif ()
#find_package(Broccoli QUIET)
#if (BROCCOLI_FOUND)
#  set(VAST_HAVE_BROCCOLI true)
#  include_directories(${BROCCOLI_INCLUDE_DIR})
#endif ()

if (NOT EDITLINE_ROOT_DIR AND VAST_PREFIX)
  set(EDITLINE_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(Editline QUIET)
if (EDITLINE_FOUND)
  set(VAST_HAVE_EDITLINE true)
  include_directories(${EDITLINE_INCLUDE_DIR})
endif ()

if (NOT Gperftools_ROOT_DIR AND VAST_PREFIX)
  set(Gperftools_ROOT_DIR ${VAST_PREFIX})
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
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/aux)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/bin
  CACHE PATH "Single directory for all executables")

set (LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/lib CACHE
  PATH "Single directory for all libraries")

add_subdirectory(aux)
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(tools)

ConfigurePackaging(${VERSION})

macro(display test desc summary)
  if (${test})
    set(${summary} ${desc})
  else ()
    set(${summary} no)
  endif()
endmacro()

display(ENABLE_DEBUG yes debug_summary)
display(DOXYGEN_FOUND yes doxygen_summary)
display(PCAP_FOUND ${PCAP_INCLUDE_DIR} pcap_summary)
display(BROCCOLI_FOUND ${BROCCOLI_INCLUDE_DIR} broccoli_summary)
display(EDITLINE_FOUND ${EDITLINE_INCLUDE_DIR} editline_summary)
display(GPERFTOOLS_FOUND ${GPERFTOOLS_INCLUDE_DIR} perftools_summary)
display(VAST_USE_PERFTOOLS_HEAP_PROFILER yes tcmalloc_summary)
display(ENABLE_ADDRESS_SANITIZER yes asan_summary)

set(build_summary
    "\n====================|  Build Summary  |===================="
    "\n"
    "\nVersion:              ${VERSION_MAJ_MIN}"
    "\n"
    "\nDebug mode:           ${debug_summary}"
    "\nBuild type:           ${CMAKE_BUILD_TYPE}"
    "\nSource directory:     ${CMAKE_SOURCE_DIR}"
    "\nBuild directory:      ${CMAKE_BINARY_DIR}"
    "\nInstall prefix:       ${CMAKE_INSTALL_PREFIX}"
    "\n"
    "\nBoost:                ${Boost_INCLUDE_DIR}"
    "\nLibcaf:               ${LIBCAF_INCLUDE_DIRS}"
    "\n"
    "\nPCAP:                 ${pcap_summary}"
    "\nBroccoli:             ${broccoli_summary}"
    "\nEditline:             ${editline_summary}"
    "\nGperftools:           ${perftools_summary}"
    "\nDoxygen:              ${doxygen_summary}"
    "\n"
    "\nUse tcmalloc:         ${tcmalloc_summary}"
    "\nUse AddressSanitizer: ${asan_summary}"
    "\n"
    "\nCC:                   ${CMAKE_C_COMPILER}"
    "\nCXX:                  ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:             ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${build_type}}"
    "\n"
    "\n===========================================================\n")

message(${build_summary})
