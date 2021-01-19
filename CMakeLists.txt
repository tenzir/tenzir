# ------------------------------------------------------------------------------
# Project Setup
# ------------------------------------------------------------------------------

# Initial scaffold

cmake_minimum_required(VERSION 3.11...3.18 FATAL_ERROR)

# Extract VERSION
if (NOT VAST_VERSION_TAG)
  execute_process(
    COMMAND "git" "describe" "--tags" "--long" "--dirty" "--abbrev=10"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    RESULT_VARIABLE git_describe_result
    OUTPUT_VARIABLE VAST_VERSION_TAG
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  # Fallback if the return value is not 0:
  if (NOT git_describe_result EQUAL 0)
    set(VAST_VERSION_TAG "1970.01.01-unknown")
  endif ()
endif ()
string(REGEX REPLACE "\\.|-" " " version_numbers ${VAST_VERSION_TAG})
separate_arguments(version_numbers)
list(GET version_numbers 0 VERSION_YEAR)
list(GET version_numbers 1 VERSION_MONTH)
list(GET version_numbers 2 VERSION_DAY)
unset(version_numbers)

project(
  VAST
  VERSION ${VERSION_YEAR}.${VERSION_MONTH}.${VERSION_DAY}
  DESCRIPTION "Visibility Across Space and Time"
  LANGUAGES C CXX)

# Avoid CMake warning in subprojects due to CMAKE_POLICY_0048
unset(PROJECT_VERSION)
unset(PROJECT_VERSION_MAJOR)
unset(PROJECT_VERSION_MINOR)
unset(PROJECT_VERSION_PATCH)

# Prohibit in-source builds
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(
    FATAL_ERROR
      "In-source builds are not allowed. Please use "
      "./configure to choose a build directory and "
      "initialize the build configuration.")
endif ()

get_directory_property(_parent PARENT_DIRECTORY)
if (_parent)
  set(vast_is_subproject ON)
else ()
  set(vast_is_subproject OFF)
endif ()
unset(_parent)

include(CTest)
if (NOT CPack_CMake_INCLUDED)
  include(CPack)
endif ()
include(CMakePrintHelpers)
include(CMakeDependentOption)
include(GNUInstallDirs)
set(CMAKE_INSTALL_CMAKEDIR
    "${CMAKE_INSTALL_LIBDIR}/cmake"
    CACHE PATH "Base directory for installing cmake specific artifacts")
set(INSTALL_VAST_CMAKEDIR "${CMAKE_INSTALL_CMAKEDIR}/vast")

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(Helpers)
include(MacDependencyPaths)

# Build configuration

set(VAST_ENABLE_ASSERTIONS_DEFAULT ON)
if (CMAKE_BUILD_TYPE STREQUAL Release)
  set(VAST_ENABLE_ASSERTIONS_DEFAULT OFF)
endif ()

option(VAST_ENABLE_ASSERTIONS "Enable Assertions"
       "${VAST_ENABLE_ASSERTIONS_DEFAULT}")
# Setting this option removes configuration dependent absolute paths from the
# VAST installation. Concretely, it enables the dynamic binary and libraries to
# use relative paths for loading their dependencies.
option(VAST_RELOCATABLE_INSTALL "Enable relocatable installations" ON)
option(VAST_DISABLE_SDT "Dont generate USDT tracepoint instrumentation" OFF)
option(VAST_USE_BUNDLED_CAF "Always use the CAF submodule" OFF)
option(ENABLE_ZEEK_TO_VAST "Build zeek-to-vast" ON)
cmake_dependent_option(
  VAST_USE_BUNDLED_BROKER "Always use the Broker submodule" OFF
  "ENABLE_ZEEK_TO_VAST" OFF)
option(VAST_STATIC_EXECUTABLE "Link VAST statically"
       $ENV{VAST_STATIC_EXECUTABLE})
option(VAST_USE_JEMALLOC "Use jemalloc instead of libc malloc"
       "${VAST_STATIC_EXECUTABLE}")
cmake_dependent_option(
  VAST_ENABLE_BUILDID "Include a unique identifier into the elf notes section"
  ON "CMAKE_EXECUTABLE_FORMAT STREQUAL ELF" OFF)
cmake_dependent_option(
  BUILD_SHARED_LIBS "Build shared libraries instead of static" ON
  "NOT VAST_STATIC_EXECUTABLE" OFF)
cmake_dependent_option(BUILD_UNIT_TESTS "Build unit tests" ON
                       "NOT CMAKE_CROSS_COMPILING" OFF)
cmake_dependent_option(VAST_NO_ARROW "Build without Apache Arrow" OFF
                       "NOT ${CMAKE_SYSTEM_NAME} STREQUAL \"FreeBSD\"" ON)
option(VAST_BUILD_LSVAST "Build the lsvast debugging tool" ON)

# Keep make output sane
set(CMAKE_VERBOSE_MAKEFILE
    false
    CACHE STRING "Show all outputs including compiler lines." FORCE)

# Force colored output for the Ninja generator
option(VAST_COLOR_NINJA "Enable ANSI-colored output for the Ninja generator."
       ON)
if ("${CMAKE_GENERATOR}" STREQUAL "Ninja" AND VAST_COLOR_NINJA)
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-fdiagnostics-color=always)
  elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    add_compile_options(-fcolor-diagnostics)
  endif ()
endif ()

if (VAST_STATIC_EXECUTABLE AND BUILD_SHARED_LIBS)
  message(FATAL_ERROR "Cannot create static binary with dynamic libraries")
endif ()

# Silence warning CMP0042
if (APPLE AND NOT DEFINED CMAKE_MACOSX_RPATH)
  set(CMAKE_SKIP_BUILD_RPATH FALSE)
  set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
  if (NOT VAST_RELOCATABLE_INSTALL)
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
  endif ()
endif ()

# Support Clang tools such as clang-tidy
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_custom_target(
  link_target ALL
  COMMAND
    ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_BINARY_DIR}/compile_commands.json
    ${CMAKE_SOURCE_DIR}/compile_commands.json)

if (VAST_RELOCATABLE_INSTALL AND NOT VAST_STATIC_EXECUTABLE)
  if (APPLE)
    list(APPEND CMAKE_INSTALL_RPATH
         "@executable_path/../${CMAKE_INSTALL_LIBDIR}")
  else ()
    list(APPEND CMAKE_INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}")
  endif ()
endif ()

# Generate C++ header from Markdown documentation.
file(
  GLOB doc_files
  RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
  "doc/cli/*.md")
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/BANNER" doc_header NEWLINE_CONSUME)
set(doc_header "${doc_header}\n#pragma once\n")
set(doc_header "${doc_header}\nnamespace vast::documentation {\n")
foreach (x ${doc_files})
  get_filename_component(basename ${x} NAME_WE)
  string(REGEX REPLACE "-" "_" variable_name ${basename})
  string(TOUPPER ${variable_name} cmake_variable_name)
  file(STRINGS ${x} content NEWLINE_CONSUME)
  set(doc_statement "constexpr auto ${variable_name} = R\"__(${content})__\"\;")
  set(doc_header "${doc_header}\n${doc_statement}\n")
endforeach ()
set(doc_header "${doc_header}\n} // namespace vast::documentation")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/libvast/vast/documentation.hpp
     ${doc_header})

# ------------------------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------------------------

# Choose a deafult log level based on build type.
if (CMAKE_BUILD_TYPE STREQUAL Release)
  set(VAST_LOG_LEVEL_DEFAULT "DEBUG")
elseif (CMAKE_BUILD_TYPE STREQUAL MinSizeRel)
  set(VAST_LOG_LEVEL_DEFAULT "DEBUG")
elseif (CMAKE_BUILD_TYPE STREQUAL RelWithDebInfo)
  set(VAST_LOG_LEVEL_DEFAULT "DEBUG")
else ()
  set(VAST_LOG_LEVEL_DEFAULT "TRACE")
endif ()

# Make sure log level is defined and all-uppercase.
set(VAST_LOG_LEVEL_DOC "maximum log level available at runtime")
if (NOT VAST_LOG_LEVEL)
  set(VAST_LOG_LEVEL
      "${VAST_LOG_LEVEL_DEFAULT}"
      CACHE STRING "${VAST_LOG_LEVEL_DOC}")
elseif (NOT VAST_LOG_LEVEL STREQUAL "$CACHE{VAST_LOG_LEVEL}")
  # Override cached variable when re-running CMake.
  string(TOUPPER "${VAST_LOG_LEVEL}" VAST_LOG_LEVEL)
  set(VAST_LOG_LEVEL
      "${VAST_LOG_LEVEL}"
      CACHE STRING "${VAST_LOG_LEVEL_DOC}" FORCE)
endif ()

set(VAST_CAF_LOG_LEVEL
    "WARNING"
    CACHE STRING ${VAST_LOG_LEVEL_DOC})

# Raise an error for invalid log levels.
set(validLogLevels
    QUIET
    ERROR
    WARNING
    INFO
    VERBOSE
    DEBUG
    TRACE)
list(FIND validLogLevels "${VAST_LOG_LEVEL}" logLevelIndex)
if (logLevelIndex LESS 0)
  message(FATAL_ERROR "Invalid log level: \"${VAST_LOG_LEVEL}\"")
endif ()

# ------------------------------------------------------------------------------
# Compiler Setup
# ------------------------------------------------------------------------------

# Compiler requirements
set(CLANG_MINIMUM_VERSION 8.0)
set(APPLE_CLANG_MINIMUM_VERSION 9.1)
set(GCC_MINIMUM_VERSION 8.0)

# Ensures that the specified C++ compiler meets our version requirements.
macro (check_compiler_version version)
  message(STATUS "Using ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${version})
    message(FATAL_ERROR "${CMAKE_CXX_COMPILER_VERSION} >= ${version} required")
  endif ()
endmacro ()

# Respect environment variables.
set(LDFLAGS "$ENV{LDFLAGS}")
set(ARFLAGS "$ENV{ARFLAGS}")

if (VAST_ENABLE_BUILDID)
  string(APPEND LDFLAGS " -Wl,--build-id")
endif ()

set(CMAKE_CXX_STANDARD 17)

# Build-type specific flags.
string(APPEND CMAKE_CXX_FLAGS_CI " -O2 -g1 -Werror -Wno-error=deprecated"
       " -Wno-error=deprecated-declarations")
string(APPEND CMAKE_CXX_FLAGS_DEBUG " -O0")
string(APPEND CMAKE_CXX_FLAGS_RELEASE
       " -msse3 -mssse3 -msse4.1 -msse4.2 -fno-omit-frame-pointer")
string(APPEND CMAKE_CXX_FLAGS_RELWITHDEBINFO " -fno-omit-frame-pointer")

# Our default flags.
set(EXTRA_FLAGS "${EXTRA_FLAGS} -Wall -Wextra -pedantic")
# Increase maximum number of template instantiations, for all that template-
# heavy code.
set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftemplate-backtrace-limit=0")
# Relocate debug/file/macro paths to a common prefix for CCache users that
# work from multiple worktrees.
set(EXTRA_FLAGS
    "${EXTRA_FLAGS} -fdebug-prefix-map=${CMAKE_CURRENT_SOURCE_DIR}=.")

# Enable more (most) warnings when requested by the user.
if (MORE_WARNINGS)
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(WFLAGS
        "-Weverything -Wno-c++98-compat -Wno-padded "
        "-Wno-documentation-unknown-command -Wno-exit-time-destructors "
        "-Wno-global-constructors -Wno-missing-prototypes "
        "-Wno-c++98-compat-pedantic -Wno-unused-member-function "
        "-Wno-unused-const-variable -Wno-switch-enum "
        "-Wno-abstract-vbase-init "
        "-Wno-missing-noreturn -Wno-covered-switch-default "
        "-Wno-unknown-warning-option")
  elseif (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    set(WFLAGS
        "-Waddress -Wall -Warray-bounds "
        "-Wattributes -Wbuiltin-macro-redefined -Wcast-align "
        "-Wcast-qual -Wchar-subscripts -Wclobbered -Wcomment "
        "-Wconversion -Wconversion-null -Wcoverage-mismatch "
        "-Wcpp -Wdelete-non-virtual-dtor -Wdeprecated "
        "-Wdeprecated-declarations -Wdiv-by-zero -Wdouble-promotion "
        "-Wempty-body -Wendif-labels -Wenum-compare -Wextra "
        "-Wfloat-equal -Wformat -Wfree-nonheap-object "
        "-Wignored-qualifiers -Winit-self "
        "-Winline -Wint-to-pointer-cast -Winvalid-memory-model "
        "-Winvalid-offsetof -Wlogical-op -Wmain -Wmaybe-uninitialized "
        "-Wmissing-braces -Wmultichar "
        "-Wnarrowing -Wnoexcept -Wnon-template-friend "
        "-Wnon-virtual-dtor -Wnonnull -Woverflow "
        "-Woverlength-strings -Wparentheses "
        "-Wpmf-conversions -Wpointer-arith -Wreorder "
        "-Wreturn-type -Wsequence-point "
        "-Wsign-compare -Wswitch -Wtype-limits -Wundef "
        "-Wuninitialized -Wunused -Wvla -Wwrite-strings "
        "-Wno-unknown-warning")
  endif ()
  # convert CMake list to a single string, erasing the ";" separators
  string(REPLACE ";" "" WFLAGS_STR ${WFLAGS})
  set(EXTRA_FLAGS "${EXTRA_FLAGS} ${WFLAGS_STR}")
endif ()

# Requirement checks
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  check_compiler_version(${GCC_MINIMUM_VERSION})
  # Too many false positives.
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -Wno-redundant-move")
  if (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
    # Works around issues with libstdc++ and C++11. For details, see: -
    # https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=194929 -
    # https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=182657
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -D_GLIBCXX_USE_C99")
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -D_GLIBCXX_USE_C99_MATH")
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -D_GLIBCXX_USE_C99_MATH_TR1")
  endif ()
elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    message(STATUS "Using Clang that ships with XCode")
    check_compiler_version(${APPLE_CLANG_MINIMUM_VERSION})
  else ()
    message(STATUS "Using third-party Clang not from XCode")
    check_compiler_version(${CLANG_MINIMUM_VERSION})
  endif ()
else ()
  message(FATAL_ERROR "Unsupported compiler: ${CMAKE_CXX_COMPILER_ID}")
endif ()

option(ENABLE_ADDRESS_SANITIZER
       "Insert pointer and reference checks into the generated binaries" OFF)
if (ENABLE_ADDRESS_SANITIZER)
  set(CXXFLAGS_BACKUP "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
  try_run(program_result compilation_succeeded "${CMAKE_BINARY_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/cmake/get_compiler_version.cpp")
  set(CMAKE_CXX_FLAGS "${CXXFLAGS_BACKUP}")
  if (NOT compilation_succeeded)
    message(STATUS "Address Sanitizer not available on selected compiler")
  else ()
    message(STATUS "Enabling Address Sanitizer")
    set(ASAN_FOUND true)
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
  endif ()
endif ()

option(ENABLE_UB_SANITIZER
       "Add run-time checks for undefined behavior into the generated binaries"
       OFF)
if (ENABLE_UB_SANITIZER)
  set(CXXFLAGS_BACKUP "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "-fsanitize=undefined")
  try_run(program_result compilation_succeeded "${CMAKE_BINARY_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/cmake/get_compiler_version.cpp")
  set(CMAKE_CXX_FLAGS "${CXXFLAGS_BACKUP}")
  if (NOT compilation_succeeded)
    message(
      STATUS "Undefined Behavior Sanitizer not available on selected compiler")
  else ()
    message(STATUS "Enabling Undefined Behavior Sanitizer")
    set(UBSAN_FOUND true)
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -fsanitize=undefined")
  endif ()
endif ()

# set -fno-exception if requested
if (NO_EXCEPTIONS)
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -fno-exceptions")
endif ()

if (ENABLE_GCOV)
  # Locate gcov and gcovr.
  find_package(GCOV)
  if (NOT GCOV_FOUND)
    message(FATAL_ERROR "Could not find gcov")
  endif ()
  find_package(GCOVR)
  if (NOT GCOVR_FOUND)
    message(FATAL_ERROR "Could not find gcovr")
  endif ()
  # For compile flags, GCC and Clang share the same options.
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -fprofile-arcs -ftest-coverage")
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -fno-omit-frame-pointer")
  # For link flags, GCC wants -lgcov and Clang --coverage.
  if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
    set(LDFLAGS "${LDFLAGS} -lgcov")
  elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    set(LDFLAGS "${LDFLAGS} --coverage")
  endif ()
  # Create a CMake script that removes all *.gcda files. We'll call this below
  # in the custom target.
  file(
    WRITE ${CMAKE_CURRENT_BINARY_DIR}/remove_gcda.cmake
    "file(GLOB_RECURSE GCDA_FILES \"${CMAKE_CURRENT_BINARY_DIR}/*.gcda\")\n"
    "if(NOT GCDA_FILES STREQUAL \"\")\n" "  file(REMOVE \${GCDA_FILES})\n"
    "endif()")
  file(GLOB_RECURSE GCOV_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/libvast/*")
  set(GCOV_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/coverage")
  # Add new target that invokes the unit test suite followed by gcov.
  add_custom_target(
    gcov
    # Reset coverage counters.
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/remove_gcda.cmake
            # Run unit tests.
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
            # Invoke gcov.
    COMMAND
      ${CMAKE_COMMAND} -DGCOV_SOURCES="${GCOV_SOURCES}"
      -DGCOV_OUTPUT_DIR="${GCOV_OUTPUT_DIR}" -P
      ${CMAKE_CURRENT_SOURCE_DIR}/cmake/GenerateGcov.cmake
    COMMENT "Generating gcov output")
  # Add gcovr targets to generate various output files.
  add_custom_target(
    gcovr-txt
    COMMAND gcovr -k -g -r .. -e libvast/test
            --object-directory=${GCOV_OUTPUT_DIR} -o coverage.txt
    COMMENT "Creating gcov text summary")
  add_custom_target(
    gcovr-xml
    COMMAND gcovr -k -g -r .. -e libvast/test
            --object-directory=${GCOV_OUTPUT_DIR} -x -o coverage.xml
    COMMENT "Creating gcov XML summary")
  add_custom_target(
    gcovr-html
    COMMAND
      gcovr -k -g -r .. -e libvast/test --object-directory=${GCOV_OUTPUT_DIR}
      --html --html-details -o coverage.html
    COMMENT "Creating gcov HTML summary")
  # Target that runs gcov and generates all summaries.
  add_custom_target(coverage)
  add_dependencies(coverage gcov)
  add_dependencies(gcovr-txt gcov)
  add_dependencies(gcovr-xml gcov)
  add_dependencies(gcovr-html gcov)
  add_dependencies(coverage gcovr-txt)
  add_dependencies(coverage gcovr-xml)
  add_dependencies(coverage gcovr-html)
endif ()

if (SHOW_TIME_REPORT)
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftime-report")
endif ()

include(CheckCXXSourceCompiles)
include(CheckLibraryExists)

# We prepend our additional flags only if VAST is not the main project we're
# building, e.g., if we're just linking again libvast.
if (NOT vast_is_subproject)
  set(CMAKE_CXX_FLAGS "${EXTRA_FLAGS} ${CMAKE_CXX_FLAGS}")
endif ()

if (LDFLAGS)
  set(CMAKE_EXE_LINKER_FLAGS ${LDFLAGS})
  set(CMAKE_SHARED_LINKER_FLAGS ${LDFLAGS})
  set(CMAKE_MODULE_LINKER_FLAGS ${LDFLAGS})
endif ()

if (ARFLAGS)
  set(CMAKE_STATIC_LINKER_FLAGS ${ARFLAGS})
endif ()

# ------------------------------------------------------------------------------
# Dependency Configuration
# ------------------------------------------------------------------------------

# Find modules are needed by the consumer in case of a static build, or if the
# linkage is PUBLIC or INTERFACE.
macro (provide_find_module name)
  message(STATUS "Providing cmake module for ${name}")
  configure_file(cmake/Find${name}.cmake ${CMAKE_BINARY_DIR} COPYONLY)
  install(
    FILES "${CMAKE_BINARY_DIR}/Find${name}.cmake"
    DESTINATION ${INSTALL_VAST_CMAKEDIR}
    COMPONENT dev)
endmacro ()

find_package(Threads REQUIRED)

if (VAST_ENABLE_ASSERTIONS)
  find_package(Backtrace REQUIRED)
endif ()

# On macOS, Homebrew is commonly used to install OpenSSL.
if (APPLE AND NOT OPENSSL_ROOT_DIR)
  find_program(HOMEBREW brew)
  if (HOMEBREW)
    execute_process(
      COMMAND ${HOMEBREW} --prefix openssl
      OUTPUT_VARIABLE OPENSSL_ROOT_DIR
      ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
    if (OPENSSL_ROOT_DIR)
      message(STATUS "Using OpenSSL from Homebrew: ${OPENSSL_ROOT_DIR}")
    endif ()
  endif ()
endif ()

find_package(Flatbuffers REQUIRED CONFIG)
string(APPEND VAST_FIND_DEPENDENCY_LIST
       "\nfind_package(Flatbuffers REQUIRED CONFIG)")
if (TARGET flatbuffers::flatbuffers)
  set(flatbuffers_target flatbuffers::flatbuffers)
elseif (NOT VAST_STATIC_EXECUTABLE AND TARGET flatbuffers::flatbuffers_shared)
  set(flatbuffers_target flatbuffers::flatbuffers_shared)
else ()
  message(FATAL_ERROR "No suiteable imported target for Flatbuffers found")
endif ()
get_target_property(flatbuffers_dir ${flatbuffers_target}
                    INTERFACE_INCLUDE_DIRECTORIES)
get_filename_component(flatbuffers_dir ${flatbuffers_dir} DIRECTORY) # <prefix>

find_package(yaml-cpp 0.6.2 REQUIRED HINTS ${YAML_CPP_ROOT})
get_filename_component(yaml_cpp_dir "${YAML_CPP_INCLUDE_DIR}" ABSOLUTE)
get_filename_component(yaml_cpp_dir "${yaml_cpp_dir}" DIRECTORY)

option(SPDLOG_FMT_EXTERNAL "Use spdlog with external libfmt" OFF)
find_package(spdlog 1.5.0 REQUIRED)
if (SPDLOG_FMT_EXTERNAL)
  find_package(fmt 5.2.1 REQUIRED)
endif()


if (NOT VAST_USE_BUNDLED_CAF)
  # Try to find the required CAF components first...
  find_package(
    CAF 0.17.6 EXACT
    COMPONENTS core io test
    QUIET)
endif ()
if (CAF_FOUND)
  message(STATUS "Found CAF")
  provide_find_module(CAF)
  string(APPEND VAST_FIND_DEPENDENCY_LIST
         "\nfind_package(CAF COMPONENTS core io test REQUIRED QUIET)")
  set(CAF_FOUND_SAVE ${CAF_FOUND})
  # ...and then optional components.
  find_package(
    CAF
    COMPONENTS openssl
    QUIET)
  if (CAF_LIBRARY_OPENSSL)
    message(STATUS "Found CAF's OpenSSL module, enabling OpenSSL support")
    set(VAST_USE_OPENSSL true)
    string(APPEND VAST_FIND_DEPENDENCY_LIST
           "\nfind_package(CAF COMPONENTS openssl REQUIRED QUIET)")
  endif ()
  set(CAF_FOUND ${CAF_FOUND_SAVE})
elseif (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/aux/caf/CMakeLists.txt)
  set(CAF_LOG_LEVEL ${VAST_CAF_LOG_LEVEL})
  set(CAF_NO_AUTO_LIBCPP TRUE)
  set(CAF_NO_OPENCL TRUE)
  set(CAF_NO_EXAMPLES TRUE)
  set(CAF_NO_UNIT_TESTS TRUE)
  set(CAF_NO_PYTHON TRUE)
  set(CAF_NO_TOOLS TRUE)
  if (BUILD_SHARED_LIBS)
    set(_linkage_suffix shared)
  else ()
    set(_linkage_suffix static)
    set(CAF_BUILD_STATIC TRUE)
    set(CAF_BUILD_STATIC_ONLY TRUE)
  endif ()
  add_subdirectory(aux/caf)
  set_target_properties(libcaf_core_${_linkage_suffix} PROPERTIES EXPORT_NAME
                                                                  core)
  add_library(caf::core ALIAS libcaf_core_${_linkage_suffix})
  set_target_properties(libcaf_io_${_linkage_suffix} PROPERTIES EXPORT_NAME io)
  target_compile_options(
    libcaf_io_${_linkage_suffix} PRIVATE -Wno-maybe-uninitialized
                                         -Wno-unknown-warning-option)
  target_link_libraries(libcaf_io_${_linkage_suffix} Threads::Threads)
  add_library(caf::io ALIAS libcaf_io_${_linkage_suffix})
  install(
    TARGETS libcaf_core_${_linkage_suffix} libcaf_io_${_linkage_suffix}
    EXPORT CAFTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  if (TARGET libcaf_openssl_${_linkage_suffix})
    target_include_directories(
      libcaf_openssl_${_linkage_suffix}
      PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/aux/caf/libcaf_openssl>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    set_target_properties(libcaf_openssl_${_linkage_suffix}
                          PROPERTIES EXPORT_NAME openssl)
    add_library(caf::openssl ALIAS libcaf_openssl_${_linkage_suffix})
    install(
      TARGETS libcaf_openssl_${_linkage_suffix}
      EXPORT CAFTargets
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
    set(VAST_USE_OPENSSL true)
  endif ()
  add_library(libcaf_test INTERFACE)
  target_link_libraries(libcaf_test INTERFACE caf::core)
  target_include_directories(
    libcaf_test
    INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/aux/caf/libcaf_test>
              $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  set_target_properties(libcaf_test PROPERTIES EXPORT_NAME test)
  add_library(caf::test ALIAS libcaf_test)
  install(
    TARGETS libcaf_test
    EXPORT CAFTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  set(caf_dir ${CMAKE_CURRENT_SOURCE_DIR}/aux/caf)
  export(
    EXPORT CAFTargets
    FILE CAFTargets.cmake
    NAMESPACE caf::)
  install(
    EXPORT CAFTargets
    DESTINATION "${INSTALL_VAST_CMAKEDIR}"
    NAMESPACE caf::)
  string(APPEND VAST_EXTRA_TARGETS_FILES
         "\ninclude(\"\${CMAKE_CURRENT_LIST_DIR}/CAFTargets.cmake\")")
  set(CAF_FOUND true)
else ()
  message(
    FATAL_ERROR
      "CAF not found, either use -DCAF_ROOT_DIR=... or initialize the aux/caf submodule"
  )
endif ()

if (ENABLE_ZEEK_TO_VAST)
  if (NOT VAST_USE_BUNDLED_BROKER)
    find_package(BROKER 1.3.1 EXACT QUIET)
  endif ()
  if (BROKER_FOUND)
    get_target_property(broker_dir zeek::broker IMPORTED_LOCATION)
    get_filename_component(broker_dir "${broker_dir}" DIRECTORY) # <prefix>/lib
    get_filename_component(broker_dir "${broker_dir}" DIRECTORY) # <prefix>
  else ()
    message(STATUS "Broker not found falling back to bundled version")
    if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/aux/broker/broker/CMakeLists.txt)
      add_subdirectory(aux/broker)
      set(broker_dir ${CMAKE_CURRENT_SOURCE_DIR}/aux/broker)
    else ()
      message(
        FATAL_ERROR
          "Broker not found, either use -DBROKER_ROOT_DIR=... or initialize the aux/broker/broker submodule"
      )
    endif ()
  endif ()
  set(VAST_HAVE_BROKER true)
endif ()

if (VAST_USE_JEMALLOC)
  find_package(jemalloc REQUIRED QUIET)
  if (jemalloc_FOUND)
    set(VAST_HAVE_JEMALLOC true)
    if (NOT BUILD_SHARED_LIBS)
      provide_find_module(jemalloc)
      string(APPEND VAST_FIND_DEPENDENCY_LIST
             "\nfind_package(jemalloc REQUIRED QUIET)")
    endif ()
  endif ()
endif ()

if (NOT PCAP_ROOT_DIR AND VAST_PREFIX)
  set(PCAP_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(PCAP QUIET)
if (PCAP_FOUND)
  set(VAST_HAVE_PCAP true)
  if (NOT BUILD_SHARED_LIBS)
    provide_find_module(PCAP)
    string(APPEND VAST_FIND_DEPENDENCY_LIST
           "\nfind_package(PCAP REQUIRED QUIET)")
  endif ()
endif ()

if (NOT VAST_NO_ARROW)
  if (NOT ARROW_ROOT_DIR AND VAST_PREFIX)
    set(ARROW_ROOT_DIR ${VAST_PREFIX})
  endif ()
  if (ARROW_ROOT_DIR)
    set(Arrow_ROOT ${ARROW_ROOT_DIR})
  endif ()
  find_package(Arrow 0.17 REQUIRED QUIET CONFIG)
  string(APPEND VAST_FIND_DEPENDENCY_LIST
         "\nfind_package(Arrow REQUIRED QUIET CONFIG)")
  set(VAST_HAVE_ARROW true)
  if (BUILD_SHARED_LIBS)
    set(ARROW_LIBRARY arrow_shared)
  else ()
    set(ARROW_LIBRARY arrow_static)
  endif ()
  get_target_property(arrow_mode ${ARROW_LIBRARY} IMPORTED_CONFIGURATIONS)
  list(GET arrow_mode 0 arrow_mode)
  get_target_property(arrow_dir ${ARROW_LIBRARY}
                      IMPORTED_LOCATION_${arrow_mode})
  get_filename_component(arrow_dir ${arrow_dir} DIRECTORY) # <prefix>/lib
  get_filename_component(arrow_dir ${arrow_dir} DIRECTORY) # <prefix>
endif ()

# Add robin_map
add_subdirectory(aux/robin-map)
export(
  EXPORT tsl-robin-mapTargets
  FILE tsl-robin-mapTargets.cmake
  NAMESPACE tsl::)
install(
  EXPORT tsl-robin-mapTargets
  DESTINATION "${INSTALL_VAST_CMAKEDIR}"
  NAMESPACE tsl::)
string(APPEND VAST_EXTRA_TARGETS_FILES
       "\ninclude(\"\${CMAKE_CURRENT_LIST_DIR}/tsl-robin-mapTargets.cmake\")")

find_package(Doxygen QUIET)
find_package(Pandoc QUIET)

# ------------------------------------------------------------------------------
# Build Setup
# ------------------------------------------------------------------------------

# make uninstall
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
               "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake" @ONLY)
add_custom_target(
  uninstall_vast COMMAND ${CMAKE_COMMAND} -P
                         ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
if (NOT TARGET uninstall)
  add_custom_target(uninstall)
endif ()
add_dependencies(uninstall uninstall_vast)

# make configure
if (NOT vast_is_subproject AND NOT TARGET configure)
  add_custom_target(configure COMMAND ${CMAKE_CURRENT_BINARY_DIR}/config.status)
endif ()

set(EXECUTABLE_OUTPUT_PATH
    ${CMAKE_CURRENT_BINARY_DIR}/bin
    CACHE PATH "Single directory for all executables")

set(LIBRARY_OUTPUT_PATH
    ${CMAKE_CURRENT_BINARY_DIR}/lib
    CACHE PATH "Single directory for all libraries")

if (VAST_HAVE_ARROW)
  set(REQUIREMENT_PYARROW "python -m pip install pyarrow")
endif ()
file(
  GENERATE
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/integration_$<CONFIG>.sh"
  CONTENT
    "#!/bin/sh
     if ! command -v jq >/dev/null 2>&1; then
       >&2 echo 'failed to find jq in $PATH'
       exit 1
     fi
     base_dir=\"${CMAKE_CURRENT_SOURCE_DIR}/integration\"
     env_dir=\"${CMAKE_CURRENT_BINARY_DIR}/integration_env\"
     app=\"$<TARGET_FILE:vast>\"
     set -e
     if [ ! -d \"$env_dir\" ]; then
       python3 -m venv \"$env_dir\"
     fi
     . \"$env_dir/bin/activate\"
     python -m pip install --upgrade pip
     python -m pip install -r \"$base_dir/requirements.txt\"
     ${REQUIREMENT_PYARROW}
     python \"$base_dir/integration.py\" --app \"$app\" -d vast-integration-test \"$@\""
)

add_custom_target(
  integration
  COMMAND /bin/sh "${CMAKE_CURRENT_BINARY_DIR}/integration_$<CONFIG>.sh"
  USES_TERMINAL)

add_custom_target(
  vast-schema
  COMMAND
    ${CMAKE_COMMAND} -E copy_directory "${CMAKE_CURRENT_SOURCE_DIR}/schema"
    "${CMAKE_BINARY_DIR}/share/vast/schema/"
  COMMENT "Copying schema directory")

add_subdirectory(libvast)
if (NOT CMAKE_CROSS_COMPILING)
  add_subdirectory(libvast_test)
endif ()
add_subdirectory(tools)
add_subdirectory(doc)
add_subdirectory(vast)
if (NOT vast_is_subproject AND TARGET man)
  add_dependencies(man vast)
endif ()

# ------------------------------------------------------------------------------
# Additional Installations
# ------------------------------------------------------------------------------

if (VAST_RELOCATABLE_INSTALL
    AND BUILD_SHARED_LIBS
    AND CAF_LIBRARY_CORE)
  # Copy CAF libraries to installation directory
  get_filename_component(CAF_LIBDIR ${CAF_LIBRARY_CORE} PATH)
  file(GLOB CAF_INSTALLED_LIBRARIES "${CAF_LIBDIR}/libcaf*.so")
  install(FILES ${CAF_INSTALLED_LIBRARIES}
          DESTINATION "${CMAKE_INSTALL_LIBDIR}/")
endif ()

export(
  EXPORT VASTTargets
  FILE VASTTargets.cmake
  NAMESPACE vast::)

install(
  EXPORT VASTTargets
  DESTINATION "${INSTALL_VAST_CMAKEDIR}"
  NAMESPACE vast::)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/VASTConfigVersion.cmake"
  VERSION ${VERSION_YEAR}.${VERSION_MONTH}.${VERSION_DAY}
  COMPATIBILITY ExactVersion)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/VASTConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/VASTConfig.cmake"
  INSTALL_DESTINATION "${INSTALL_VAST_CMAKEDIR}")

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/VASTConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/VASTConfigVersion.cmake"
        DESTINATION "${INSTALL_VAST_CMAKEDIR}")

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/schema"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/vast")

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/integration"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/vast"
        PATTERN "integration/vast.yaml.example" EXCLUDE)
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/vast.yaml.example"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/integration")

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE"
        DESTINATION "${CMAKE_INSTALL_DOCDIR}/")

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/README.md"
        DESTINATION "${CMAKE_INSTALL_DOCDIR}/")

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/vast.yaml.example"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/")

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/scripts/corelight-cat"
        DESTINATION ${CMAKE_INSTALL_BINDIR})

# Install rc.d script on FreeBSD into PREFIX/etc/rc.d.
if (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
  file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/scripts/freebsd-rc-vast"
       rc_template NEWLINE_CONSUME)
  string(REPLACE "%%PREFIX%%" ${CMAKE_INSTALL_PREFIX} rc_content ${rc_template})
  set(rc_file "${CMAKE_CURRENT_BINARY_DIR}/freebsd-rc-vast")
  file(WRITE ${rc_file} ${rc_content})
  install(
    FILES "${rc_file}"
    DESTINATION "${CMAKE_INSTALL_PREFIX}/etc/rc.d"
    RENAME "vast"
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_EXECUTE)
endif ()

# ------------------------------------------------------------------------------
# Build Summary
# ------------------------------------------------------------------------------

macro (display TEST desc summary)
  if (${TEST})
    set(${summary} ${desc})
  else ()
    set(${summary} no)
  endif ()
endmacro ()

display(SHOW_TIME_REPORT yes time_report_summary)
display(VAST_ENABLE_ASSERTIONS yes assertions_summary)
display(ASAN_FOUND yes asan_summary)
display(UBSAN_FOUND yes ubsan_summary)
display(ENABLE_GCOV yes gcov_summary)

# Figure out whether we point to a build directory or a prefix.
if (NOT caf_dir)
  set(caf_dir ${CAF_LIBRARY_CORE})
  get_filename_component(caf_dir ${caf_dir} DIRECTORY)
  get_filename_component(caf_dir ${caf_dir} DIRECTORY)
endif ()

display(CAF_FOUND "${caf_dir}" caf_summary)
display(VAST_HAVE_BROKER "${broker_dir}" broker_summary)
display(Arrow_FOUND "${arrow_dir}" arrow_summary)
display(PCAP_FOUND "${PCAP_INCLUDE_DIR}" pcap_summary)
display(YAML_CPP_INCLUDE_DIR "${yaml_cpp_dir}" yaml_cpp_summary)
display(DOXYGEN_FOUND yes doxygen_summary)
display(PANDOC_FOUND yes pandoc_summary)
display(VAST_USE_JEMALLOC "${jemalloc_INCLUDE_DIR}" jemalloc_summary)
display(VAST_USE_OPENSSL yes openssl_summary)
display(BUILD_UNIT_TESTS yes build_tests_summary)
display(VAST_RELOCATABLE_INSTALL yes relocatable_install_summary)

string(TOUPPER "${CMAKE_BUILD_TYPE}" build_type)
set(summary
    "====================|  VAST Build Summary  |===================="
    "\n"
    "\nVersion:             ${VAST_VERSION_TAG}"
    "\n"
    "\nSource directory:    ${CMAKE_SOURCE_DIR}"
    "\nBuild directory:     ${CMAKE_BINARY_DIR}"
    "\nInstall prefix:      ${CMAKE_INSTALL_PREFIX}"
    "\n"
    "\nBuild type:          ${CMAKE_BUILD_TYPE}"
    "\nLog level:           ${VAST_LOG_LEVEL}"
    "\nRelocatable install: ${relocatable_install_summary}"
    "\nBuild unit tests:    ${build_tests_summary}"
    "\nShow time report:    ${time_report_summary}"
    "\nAssertions:          ${assertions_summary}"
    "\nAddressSanitizer:    ${asan_summary}"
    "\nUBSanitizer:         ${ubsan_summary}"
    "\ngcov:                ${gcov_summary}"
    "\n"
    "\nC compiler:          ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}"
    "\nC++ compiler:        ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}"
    "\n"
    "\nCC:                  ${CMAKE_C_COMPILER}"
    "\nCXX:                 ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:            ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${build_type}}"
    "\nLDFLAGS:             ${LDFLAGS}"
    "\n"
    "\nFlatBuffers:         ${flatbuffers_dir}"
    "\nyaml-cpp:            ${yaml_cpp_summary}"
    "\nCAF:                 ${caf_summary}"
    "\nArrow:               ${arrow_summary}"
    "\nBroker:              ${broker_summary}"
    "\nPCAP:                ${pcap_summary}"
    "\nDoxygen:             ${doxygen_summary}"
    "\npandoc:              ${pandoc_summary}"
    "\n"
    "\njemalloc:            ${jemalloc_summary}"
    "\nOpenSSL:             ${openssl_summary}"
    "\n"
    "\n================================================================")

message("\n" ${summary} "\n")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/config.summary ${summary})
