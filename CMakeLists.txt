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

set(CMAKE_CXX_FLAGS                "-Wall -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os")
set(CMAKE_CXX_FLAGS_RELEASE        "-O4")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")

if (ENABLE_DEBUG)
    set(CMAKE_BUILD_TYPE Debug)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DVAST_DEBUG")
else ()
    set(CMAKE_BUILD_TYPE Release)
endif ()

if("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
    execute_process(
        COMMAND ${CMAKE_CXX_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)
    if (NOT (GCC_VERSION VERSION_GREATER 4.7 OR GCC_VERSION VERSION_EQUAL 4.7))
        message(FATAL_ERROR "${PROJECT_NAME} requires g++ 4.7 or greater.")
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
find_package(Boost
    1.50.0
    COMPONENTS
        filesystem
        program_options
        system
        unit_test_framework
)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
else ()
    message(FATAL_ERROR "Could not find Boost")
endif ()

find_package(Broccoli)
if (BROCCOLI_FOUND)
    include_directories(${Broccoli_INCLUDE_DIR})
else ()
    message(FATAL_ERROR "Could not find Broccoli")
endif ()

find_package(Libcppa)
if (LIBCPPA_FOUND)
    include_directories(${LIBCPPA_INCLUDE_DIR})
else ()
    message(FATAL_ERROR "Could not find libcppa")
endif ()

find_package(0event)
if (0EVENT_FOUND)
    include_directories(${0event_INCLUDE_DIR})
else ()
    message(FATAL_ERROR "Could not find 0event")
endif ()

find_package(Doxygen)
if (DOXYGEN_FOUND)
    add_subdirectory(doc)
endif ()

find_package(Gperftools)
if (GPERFTOOLS_FOUND)
  set(USE_PERFTOOLS_CPU_PROFILER true)
  include_directories(BEFORE ${GPERFTOOLS_INCLUDE_DIR})
endif ()
set(USE_PERFTOOLS_HEAP_PROFILER false)
if (ENABLE_PERFTOOLS_HEAP)
  set(USE_PERFTOOLS_HEAP_PROFILER true)
endif ()

include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(src)
add_subdirectory(test)

# If this CMake project is a sub-project of another, we will not
# configure the generic packaging because CPack will fail in the case
# that the parent project has already configured packaging
if ("${PROJECT_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
    include(ConfigurePackaging)
    ConfigurePackaging(${VERSION})
endif ()

string(TOUPPER ${CMAKE_BUILD_TYPE} build_type)
message(
    "\n====================|  Build Summary  |===================="
    "\n"
    "\nVAST version:     ${VERSION_MAJ_MIN}"
    "\n"
    "\nSource directory: ${CMAKE_SOURCE_DIR}"
    "\nBuild directory:  ${CMAKE_BINARY_DIR}"
    "\nInstall prefix:   ${CMAKE_INSTALL_PREFIX}"
    "\nDebug mode:       ${ENABLE_DEBUG}"
    "\nGperftools:       ${GPERFTOOLS_FOUND}"
    "\nGperftools heap:  ${USE_PERFTOOLS_HEAP_PROFILER}"
    "\n"
    "\nCXX:              ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:         ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${build_type}}"
    "\n"
    "\n0event:           ${0event_INCLUDE_DIR}"
    "\nLibcppa:          ${LIBCPPA_INCLUDE_DIR}"
    "\nBoost:            ${Boost_INCLUDE_DIR}"
    "\nDoxygen:          ${DOXYGEN_FOUND}"
    "\n"
    "\n===========================================================\n"
)
