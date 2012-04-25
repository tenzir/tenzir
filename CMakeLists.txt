project(VAST CXX)

#-----------------------------------------------------------------------
#                             CMake Configuration
#-----------------------------------------------------------------------
cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

# Prohibit in-source builds.
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
    message(FATAL_ERROR "In-source builds are not allowed. Please use "
                        "./configure to choose a build directory and "
                        "initialize the build configuration.")
endif ()

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(Helpers)

if ("${PROJECT_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
    # Support 'make uninstall'.
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
                   "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
                    @ONLY)

    add_custom_target(uninstall COMMAND
        ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
endif ()

# Ignore noisy compiler output.
set(CMAKE_VERBOSE_MAKEFILE false
    CACHE STRING "Show all outputs including compiler lines." FORCE)

#-----------------------------------------------------------------------
#                             Project Configuration
#-----------------------------------------------------------------------

# Extract version.
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" VERSION LIMIT_COUNT 1)
string(REPLACE "." " " version_numbers ${VERSION})
separate_arguments(version_numbers)
list(GET version_numbers 0 VERSION_MAJOR)
list(GET version_numbers 1 VERSION_MINOR)
set(VERSION_MAJ_MIN "${VERSION_MAJOR}.${VERSION_MINOR}")

set(CMAKE_CXX_FLAGS                "-Wall -std=c++11")
set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE        "-O4 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")

if (ENABLE_DEBUG)
    set(CMAKE_BUILD_TYPE Debug)
    add_definitions("-DVAST_DEBUG")
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

#-----------------------------------------------------------------------
#                          Dependency Configuration
#-----------------------------------------------------------------------

include(MacDependencyPaths)

find_package(Boost
    1.49.0
    COMPONENTS
        filesystem
        program_options
        system
        unit_test_framework
)
if (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
else ()
    set(MISSING_PREREQS true)
    list(APPEND MISSING_DEPENDENCIES Boost)
endif ()

find_package(Broccoli)
if (BROCCOLI_FOUND)
    include_directories(${Broccoli_INCLUDE_DIR})
else ()
    set(MISSING_PREREQS true)
    list(APPEND MISSING_DEPENDENCIES Broccoli)
endif ()

find_package(ZeroMQ)
if (ZEROMQ_FOUND)
    include_directories(${ZeroMQ_INCLUDE_DIR})
else ()
    set(MISSING_PREREQS true)
    list(APPEND MISSING_DEPENDENCIES ZeroMQ)
endif ()

find_package(0event)
if (0EVENT_FOUND)
    include_directories(${0event_INCLUDE_DIR})
else ()
    set(MISSING_PREREQS true)
    list(APPEND MISSING_DEPENDENCIES 0event)
endif ()

if (MISSING_PREREQS)
    set (deps)
    foreach (dep ${MISSING_DEPENDENCIES})
        set(deps "${deps} ${dep}")
    endforeach ()
    message(FATAL_ERROR "Could not find required dependencies:${deps}")
endif ()

#
# Optional dependencies
#

find_package(Doxygen)

set(USE_PERFTOOLS false)
if (ENABLE_PERFTOOLS)
    find_package(Perftools)
    if (PERFTOOLS_FOUND)
        set(USE_PERFTOOLS true)
        include_directories(BEFORE ${Perftools_INCLUDE_DIR})
        list(APPEND OPTLIBS ${Perftools_LIBRARIES})
    endif ()
endif ()

#-----------------------------------------------------------------------
#                            System Introspection
#-----------------------------------------------------------------------

include_directories(${CMAKE_CURRENT_BINARY_DIR})

#-----------------------------------------------------------------------
#                         Recurse into Subdirectories
#-----------------------------------------------------------------------

add_subdirectory(src)
add_subdirectory(test)
if (DOXYGEN_FOUND)
    add_subdirectory(doc)
endif ()

#-----------------------------------------------------------------------
#                           Packaging Configuration
#-----------------------------------------------------------------------

# If this CMake project is a sub-project of another, we will not
# configure the generic packaging because CPack will fail in the case
# that the parent project has already configured packaging
if ("${PROJECT_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
    include(ConfigurePackaging)
    ConfigurePackaging(${VERSION})
endif ()

#-----------------------------------------------------------------------
#                             Build Configuration
#-----------------------------------------------------------------------

string(TOUPPER ${CMAKE_BUILD_TYPE} build_type)
get_directory_property(defs DIRECTORY ${CMAKE_SOURCE_DIR} COMPILE_DEFINITIONS)
join("${defs}" " " definitions)

message(
    "\n====================|  VAST Build Summary  |===================="
    "\n"
    "\nInstall prefix:    ${CMAKE_INSTALL_PREFIX}"
    "\nDebug mode:        ${ENABLE_DEBUG}"
    "\n"
    "\nCXX:               ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:          ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${build_type}}"
    "\nDefinitions:       ${definitions}"
    "\n"
    "\nGoogle perftools:  ${USE_PERFTOOLS}"
    "\n"
    "\n================================================================\n"
)

#-----------------------------------------------------------------------
#                                Installation
#-----------------------------------------------------------------------

# Show warning when installing user is different from the one that configured
# TODO.
install(CODE "
    if (NOT $ENV{USER} STREQUAL \$ENV{USER})
        message(STATUS \"ATTENTION: Install is being performed by user \"
                \"'\$ENV{USER}', but the build directory was configured by \"
                \"user '$ENV{USER}'. This may result in a permissions error \"
                \"when writing the install manifest, but you can ignore it \"
                \"and consider the installation as successful if you don't \"
                \"care about the install manifest.\")
    endif ()
")
