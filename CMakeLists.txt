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

# Reconfigure the project with 'make configure'.
add_custom_target(configure COMMAND ${CMAKE_CURRENT_BINARY_DIR}/config.status)

set(CMAKE_VERBOSE_MAKEFILE false
  CACHE STRING "Show all outputs including compiler lines." FORCE)

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" VERSION LIMIT_COUNT 1)
string(REPLACE "." " " version_numbers ${VERSION})
separate_arguments(version_numbers)
list(GET version_numbers 0 VERSION_MAJOR)
list(GET version_numbers 1 VERSION_MINOR)
set(VERSION_MAJ_MIN "${VERSION_MAJOR}.${VERSION_MINOR}")

if (NOT CMAKE_CXX_FLAGS)
  # -ftemplate-depth=256 ot make Boost Spirit happy.
  set(EXTRA_FLAGS "-Wall -Wextra -pedantic -std=c++1y -ftemplate-depth=256")
  set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
  set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os")
  set(CMAKE_CXX_FLAGS_RELEASE        "-O3")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")
endif()

# Perform compiler requirement checks.
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
  if (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
    # Works around issues with libstdc++ and C++11. For details, see:
    # - https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=194929
    # - https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=182657
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -D_GLIBCXX_USE_C99")
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -D_GLIBCXX_USE_C99_MATH")
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -D_GLIBCXX_USE_C99_MATH_TR1")
  endif ()
elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  if (compiler_version VERSION_GREATER 3.3)
    message(STATUS "Found Clang ${compiler_version}")
  else ()
    message(FATAL_ERROR "Clang >= 3.4 required, found ${compiler_version}")
  endif ()
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -stdlib=libc++")
else ()
  message(FATAL_ERROR "unsupported compiler: ${CMAKE_CXX_COMPILER_ID}")
endif ()

if (ENABLE_ADDRESS_SANITIZER)
  set(CXXFLAGS_BACKUP "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
  try_run(program_result
          compilation_succeeded
          "${CMAKE_BINARY_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/cmake/get_compiler_version.cpp")
  if(NOT compilation_succeeded)
    message(STATUS "Address Sanitizer not available on selected compiler")
  else()
    message(STATUS "Enabling Address Sanitizer")
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    set(ASAN_FOUND true)
  endif()
  set(CMAKE_CXX_FLAGS "${CXXFLAGS_BACKUP}")
endif(ENABLE_ADDRESS_SANITIZER)

if (SHOW_TIME_REPORT)
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftime-report")
endif ()

# Only set our flags if the environment variable CXXFLAGS is not present.
if (NOT CMAKE_CXX_FLAGS)
  set(CMAKE_CXX_FLAGS ${EXTRA_FLAGS})
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

if (NOT Gperftools_ROOT_DIR AND VAST_PREFIX)
  set(Gperftools_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(Gperftools QUIET)
if (GPERFTOOLS_FOUND)
  set(VAST_HAVE_GPERFTOOLS true)
  include_directories(BEFORE ${GPERFTOOLS_INCLUDE_DIR})
endif ()

find_package(Doxygen QUIET)
find_package(Md2man QUIET)

include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR})
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/aux)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/bin
  CACHE PATH "Single directory for all executables")

set (LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/lib CACHE
  PATH "Single directory for all libraries")

add_subdirectory(aux)
add_subdirectory(doc)
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

# Prettify build summary.
display(SHOW_TIME_REPORT yes time_report_summary)
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
set(caf_dir ${CAF_LIBRARY_CORE})
get_filename_component(caf_dir ${caf_dir} PATH)
if (EXISTS "${caf_dir}/../libcaf_core")
  get_filename_component(caf_dir ${caf_dir} PATH)
else ()
  set(caf_dir ${CAF_INCLUDE_DIR_CORE})
endif ()
display(CAF_FOUND ${caf_dir} caf_summary)
display(PCAP_FOUND ${PCAP_INCLUDE_DIR} pcap_summary)
display(GPERFTOOLS_FOUND ${GPERFTOOLS_INCLUDE_DIR} perftools_summary)
display(DOXYGEN_FOUND yes doxygen_summary)
display(MD2MAN_FOUND yes md2man_summary)
display(VAST_USE_TCMALLOC yes tcmalloc_summary)
display(VAST_ENABLE_ASSERTIONS yes assertions_summary)
display(ASAN_FOUND yes asan_summary)
# Display the right include directory based on wheter we pointed to a CAF build
# directory or to an install prefix.

set(build_summary
    "\n====================|  Build Summary  |===================="
    "\n"
    "\nVersion:              ${VERSION_MAJ_MIN}"
    "\n"
    "\nSource directory:     ${CMAKE_SOURCE_DIR}"
    "\nBuild directory:      ${CMAKE_BINARY_DIR}"
    "\nInstall prefix:       ${CMAKE_INSTALL_PREFIX}"
    "\n"
    "\nBuild type:           ${CMAKE_BUILD_TYPE}"
    "\nShow time report:     ${time_report_summary}"
    "\nAssertions:           ${assertions_summary}"
    "\nLog level:            ${log_level_summary}"
    "\n"
    "\nCC:                   ${CMAKE_C_COMPILER}"
    "\nCXX:                  ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:             ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${build_type}}"
    "\n"
    "\nBoost:                ${Boost_INCLUDE_DIR}"
    "\nCAF:                  ${caf_summary}"
    "\nPCAP:                 ${pcap_summary}"
    "\nGperftools:           ${perftools_summary}"
    "\nDoxygen:              ${doxygen_summary}"
    "\nmd2man:               ${md2man_summary}"
    "\n"
    "\nUse tcmalloc:         ${tcmalloc_summary}"
    "\nUse AddressSanitizer: ${asan_summary}"
    "\n"
    "\n===========================================================\n")

message(${build_summary})
