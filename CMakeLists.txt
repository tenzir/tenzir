cmake_minimum_required(VERSION 2.6 FATAL_ERROR)
project(VAST C CXX)

# Silence warning CMP0042.
if (APPLE AND NOT DEFINED CMAKE_MACOSX_RPATH)
  set(CMAKE_MACOSX_RPATH true)
endif()

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
  # -ftemplate-depth=256 makes Boost Spirit happy.
  set(CXX_FLAGS "-Wall -Wextra -pedantic -std=c++1y -ftemplate-depth=256")
  set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
  set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os")
  set(CMAKE_CXX_FLAGS_RELEASE        "-O3")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")
endif()

if (ENABLE_DEBUG)
  set(CMAKE_BUILD_TYPE Debug)
endif ()

try_run(program_result
        compilation_succeeded
        "${CMAKE_BINARY_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/get_compiler_version.cpp"
        RUN_OUTPUT_VARIABLE compiler_version)
if (NOT compilation_succeeded OR NOT program_result EQUAL 0)
  message(FATAL_ERROR "Cannot determine compiler version")
elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  if (compiler_version VERSION_GREATER 4.8)
    message(STATUS "Found GCC ${compiler_version}")
  else ()
    message(FATAL_ERROR "GCC >= 4.9 required, found ${compiler_version}")
  endif ()
  set(CXX_FLAGS "${CXX_FLAGS} -fdiagnostics-color=always")
  if (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
    # Works around issues with libstdc++ and C++11. For details, see:
    # - https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=194929
    # - https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=182657
    set(CXX_FLAGS "${CXX_FLAGS} -D_GLIBCXX_USE_C99")
    set(CXX_FLAGS "${CXX_FLAGS} -D_GLIBCXX_USE_C99_MATH")
    set(CXX_FLAGS "${CXX_FLAGS} -D_GLIBCXX_USE_C99_MATH_TR1")
  endif ()
elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  if (compiler_version VERSION_GREATER 3.3)
    message(STATUS "Found Clang ${compiler_version}")
  else ()
    message(FATAL_ERROR "Clang >= 3.4 required, found ${compiler_version}")
  endif ()
  set(CXX_FLAGS "${CXX_FLAGS} -stdlib=libc++")
  if (ENABLE_ADDRESS_SANITIZER)
    set(CXX_FLAGS "${CXX_FLAGS} -fsanitize=address -g")
  endif ()
else ()
  message(FATAL_ERROR "unsupported compiler: ${CMAKE_CXX_COMPILER_ID}")
endif ()

# Only set our flags if the environment variable CXXFLAGS is not present.
if (NOT CMAKE_CXX_FLAGS)
  set(CMAKE_CXX_FLAGS ${CXX_FLAGS})
endif ()

find_package(Threads)
if (NOT Threads_FOUND)
  message(FATAL_ERROR "Could not find system threading libraries")
endif ()

if (BOOST_ROOT)
  set(Boost_NO_SYSTEM_PATHS true)
elseif (VAST_PREFIX)
  set(BOOST_ROOT ${VAST_PREFIX})
endif ()
find_package(Boost 1.50.0)
if (Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
else ()
  message(FATAL_ERROR "Could not find Boost")
endif ()

if (NOT CAF_ROOT_DIR AND VAST_PREFIX)
  set(CAF_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(CAF COMPONENTS core io)
if (CAF_FOUND)
  include_directories(${CAF_INCLUDE_DIRS})
else ()
  message(FATAL_ERROR "Could not find CAF")
endif ()

if (NOT PCAP_ROOT_DIR AND VAST_PREFIX)
  set(PCAP_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(PCAP QUIET)
if (PCAP_FOUND)
  set(VAST_HAVE_PCAP true)
  include_directories(${PCAP_INCLUDE_DIR})
endif ()

if (NOT EDITLINE_ROOT_DIR AND VAST_PREFIX)
  set(EDITLINE_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(Editline QUIET)
if (EDITLINE_FOUND)
  if (EDITLINE_VERSION_MAJOR LESS 2 OR EDITLINE_VERSION_MINOR LESS 11)
    message(FATAL_ERROR "libedit too old: ${EDITLINE_VERSION}, need >= 2.11")
  endif ()
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

find_package(Doxygen QUIET)
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
display(EDITLINE_FOUND
        "${EDITLINE_INCLUDE_DIR} (${EDITLINE_VERSION})" editline_summary)
display(GPERFTOOLS_FOUND ${GPERFTOOLS_INCLUDE_DIR} perftools_summary)
display(VAST_USE_PERFTOOLS_HEAP_PROFILER yes tcmalloc_summary)
display(ENABLE_ADDRESS_SANITIZER yes asan_summary)
# Display the right include directory based on wheter we pointed to a CAF build
# directory or to an install prefix.
set(caf_dir ${CAF_INCLUDE_DIR_CORE})
if (EXISTS "${CAF_INCLUDE_DIR_CORE}/../build/libcaf_core")
  get_filename_component(caf_dir ${caf_dir} PATH)
endif ()
display(CAF_FOUND ${caf_dir} caf_summary)

if (VAST_LOG_LEVEL EQUAL 0)
  set(log_level_summary quiet)
elseif (VAST_LOG_LEVEL EQUAL 1)
  set(log_level_summary error)
elseif (VAST_LOG_LEVEL EQUAL 2)
  set(log_level_summary warn)
elseif (VAST_LOG_LEVEL EQUAL 3)
  set(log_level_summary info)
elseif (VAST_LOG_LEVEL EQUAL 4)
  set(log_level_summary verbose)
elseif (VAST_LOG_LEVEL EQUAL 5)
  set(log_level_summary debug)
elseif (VAST_LOG_LEVEL EQUAL 6)
  set(log_level_summary trace)
else ()
  message(FATAL "invalid log level")
endif ()

set(build_summary
    "\n====================|  Build Summary  |===================="
    "\n"
    "\nVersion:              ${VERSION_MAJ_MIN}"
    "\n"
    "\nBuild type:           ${CMAKE_BUILD_TYPE}"
    "\nDebug mode:           ${debug_summary}"
    "\nLog level:            ${log_level_summary}"
    "\n"
    "\nCC:                   ${CMAKE_C_COMPILER}"
    "\nCXX:                  ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:             ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${build_type}}"
    "\n"
    "\nSource directory:     ${CMAKE_SOURCE_DIR}"
    "\nBuild directory:      ${CMAKE_BINARY_DIR}"
    "\nInstall prefix:       ${CMAKE_INSTALL_PREFIX}"
    "\n"
    "\nBoost:                ${Boost_INCLUDE_DIR}"
    "\nCAF:                  ${caf_summary}"
    "\nPCAP:                 ${pcap_summary}"
    "\nEditline:             ${editline_summary}"
    "\nGperftools:           ${perftools_summary}"
    "\nDoxygen:              ${doxygen_summary}"
    "\n"
    "\nUse tcmalloc:         ${tcmalloc_summary}"
    "\nUse AddressSanitizer: ${asan_summary}"
    "\n"
    "\n===========================================================\n")

message(${build_summary})
