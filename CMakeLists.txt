#------------------------------------------------------------------------------
#                                Project Setup
#------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.11 FATAL_ERROR)
project(VAST C CXX)

option(VAST_STATIC_EXECUTABLE "Link VAST statically")
option(VAST_RELOCATEABLE_INSTALL "Make the installation relocateable" OFF)
set(BUILD_SHARED_LIBS_DEFAULT ON)
if (VAST_STATIC_EXECUTABLE)
  set(BUILD_SHARED_LIBS_DEFAULT OFF)
endif ()
option(BUILD_SHARED_LIBS "" ${BUILD_SHARED_LIBS_DEFAULT})
if (VAST_STATIC_EXECUTABLE AND BUILD_SHARED_LIBS)
  MESSAGE(FATAL_ERROR "Cannot create static binary with dynamic libraries")
endif ()

# Silence warning CMP0042
if (APPLE AND NOT DEFINED CMAKE_MACOSX_RPATH)
  set(CMAKE_SKIP_BUILD_RPATH  FALSE)
  set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
  if (VAST_RELOCATEABLE_INSTALL)
    list(APPEND CMAKE_INSTALL_RPATH "@executable_path/../lib")
  else ()
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
  endif ()
endif ()

# Support Clang tools such as clang-tidy
set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(CTest)
include(Helpers)
include(MacDependencyPaths)

include(GNUInstallDirs)
set(CMAKE_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake" CACHE PATH
  "Base directory for installing cmake specific artifacts"
)
set(INSTALL_VAST_CMAKEDIR "${CMAKE_INSTALL_CMAKEDIR}/vast")

# Prohibit in-source builds
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed. Please use "
    "./configure to choose a build directory and "
    "initialize the build configuration.")
endif ()

# Keep make output sane
set(CMAKE_VERBOSE_MAKEFILE false
    CACHE STRING "Show all outputs including compiler lines." FORCE)

# Extract VERSION
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" VAST_VERSION LIMIT_COUNT 1)
string(REPLACE "." " " version_numbers ${VAST_VERSION})
separate_arguments(version_numbers)
list(GET version_numbers 0 VERSION_MAJOR)
list(GET version_numbers 1 VERSION_MINOR)
set(VERSION_MAJ_MIN "${VERSION_MAJOR}.${VERSION_MINOR}")

#------------------------------------------------------------------------------
#                                Logging Setup
#------------------------------------------------------------------------------

# Choose a deafult log level based on build type.
if (CMAKE_BUILD_TYPE STREQUAL Release)
  set(VAST_LOG_LEVEL_DEFAULT "INFO")
elseif (CMAKE_BUILD_TYPE STREQUAL RelWithDebInfo)
  set(VAST_LOG_LEVEL_DEFAULT "DEBUG")
else ()
  set(VAST_LOG_LEVEL_DEFAULT "TRACE")
endif ()

# Make sure log level is defined and all-uppercase.
set(VAST_LOG_LEVEL_DOC "maximum log level available at runtime")
if (NOT VAST_LOG_LEVEL)
  set(VAST_LOG_LEVEL "${VAST_LOG_LEVEL_DEFAULT}"
      CACHE STRING "${VAST_LOG_LEVEL_DOC}")
elseif (NOT VAST_LOG_LEVEL STREQUAL "$CACHE{VAST_LOG_LEVEL}")
  # Override cached variable when re-running CMake.
  string(TOUPPER "${VAST_LOG_LEVEL}" VAST_LOG_LEVEL)
  set(VAST_LOG_LEVEL "${VAST_LOG_LEVEL}"
      CACHE STRING "${VAST_LOG_LEVEL_DOC}"
      FORCE)
endif ()

# Raise an error for invalid log levels.
set(validLogLevels QUIET ERROR WARNING INFO DEBUG TRACE)
list(FIND validLogLevels "${VAST_LOG_LEVEL}" logLevelIndex)
if(logLevelIndex LESS 0)
  MESSAGE(FATAL_ERROR "Invalid log level: \"${VAST_LOG_LEVEL}\"")
endif()

#------------------------------------------------------------------------------
#                               Compiler Setup
#------------------------------------------------------------------------------

# Compiler requirements
set(CLANG_MINIMUM_VERSION 5.0)
set(APPLE_CLANG_MINIMUM_VERSION 9.1)
set(GCC_MINIMUM_VERSION 8.0)

# Ensures that the specified C++ compiler meets our version requirements.
macro(check_compiler_version version)
  message(STATUS "Using ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${version})
    message(FATAL_ERROR "${CMAKE_CXX_COMPILER_VERSION} >= ${version} required")
  endif ()
endmacro()

# Respect environment variables.
set(LDFLAGS "$ENV{LDFLAGS}")

# Ninja doesn't colorize compiler diagnostics by default.
if (CMAKE_GENERATOR STREQUAL "Ninja")
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -fdiagnostics-color")
endif ()

# To give the user full control, we don't mess with with CXX_FLAGS if provided.
# This is a deliberate decision but contrasts to many other packages that
# append flags to the user-provide environment variable.
if (NOT CMAKE_CXX_FLAGS)
  # Our default flags.
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -std=c++17 -Wall -Wextra -pedantic")
  set(PERFORMANCE_FLAGS "-msse3 -mssse3 -msse4.1 -msse4.2")
  # Increase maximum number of template instantiations, for all that
  # template-heavy code.
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftemplate-depth=512")
  # Reduce the number of template instantiations shown in backtrace to keep the
  # already insane error messages readable.
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -ftemplate-backtrace-limit=3")
  # Build-type specific flags.
  set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
  set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os")
  set(CMAKE_CXX_FLAGS_RELEASE        "-O3 ${PERFORMANCE_FLAGS}")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g -fno-omit-frame-pointer")
endif ()

# Enable more (most) warnings when requested by the user.
if (MORE_WARNINGS)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(WFLAGS "-Weverything -Wno-c++98-compat -Wno-padded "
               "-Wno-documentation-unknown-command -Wno-exit-time-destructors "
               "-Wno-global-constructors -Wno-missing-prototypes "
               "-Wno-c++98-compat-pedantic -Wno-unused-member-function "
               "-Wno-unused-const-variable -Wno-switch-enum "
               "-Wno-abstract-vbase-init "
               "-Wno-missing-noreturn -Wno-covered-switch-default")
  elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    set(WFLAGS "-Waddress -Wall -Warray-bounds "
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
               "-Wuninitialized -Wunused -Wvla -Wwrite-strings")
  endif()
  # convert CMake list to a single string, erasing the ";" separators
  string(REPLACE ";" "" WFLAGS_STR ${WFLAGS})
  set(EXTRA_FLAGS "${EXTRA_FLAGS} ${WFLAGS_STR}")
endif()

# Requirement checks
if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  check_compiler_version(${GCC_MINIMUM_VERSION})
  # Too many false positives.
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -Wno-maybe-uninitialized")
  if (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
    # Works around issues with libstdc++ and C++11. For details, see:
    # - https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=194929
    # - https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=182657
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -D_GLIBCXX_USE_C99")
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -D_GLIBCXX_USE_C99_MATH")
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -D_GLIBCXX_USE_C99_MATH_TR1")
  endif ()
  if (VAST_USE_TCMALLOC)
    # GCC makes some optimizations assuming it is using its own, built-in
    # malloc; that assumption obviously isn't true with tcmalloc.
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -fno-builtin-malloc -fno-builtin-calloc")
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -fno-builtin-realloc -fno-builtin-free")
  endif ()
elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    message(STATUS "Using Clang that ships with XCode")
    check_compiler_version(${APPLE_CLANG_MINIMUM_VERSION})
  else ()
    message(STATUS "Using third-party Clang not from XCode")
    check_compiler_version(${CLANG_MINIMUM_VERSION})
  endif ()
  # Standard library selection: check if libc++ is available, and if so, use
  # it. Otherwise fall back to libstdc++.
  if (NOT NO_AUTO_LIBCPP)
    set(CXXFLAGS_BACKUP "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "-std=c++17 -stdlib=libc++")
    try_run(program_result
            compilation_succeeded
            "${CMAKE_BINARY_DIR}"
            "${CMAKE_CURRENT_SOURCE_DIR}/cmake/get_compiler_version.cpp")
    set(CMAKE_CXX_FLAGS "${CXXFLAGS_BACKUP}")
    if(NOT compilation_succeeded)
      message(STATUS "Using libstdc++ as standard library")
    else()
      message(STATUS "Automatically adding -stdlib=libc++ flag")
      set(EXTRA_FLAGS "${EXTRA_FLAGS} -stdlib=libc++")
    endif()
  endif()
else ()
  message(FATAL_ERROR "Unsupported compiler: ${CMAKE_CXX_COMPILER_ID}")
endif ()

if (ENABLE_ADDRESS_SANITIZER)
  set(CXXFLAGS_BACKUP "${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS "-fsanitize=address -fno-omit-frame-pointer")
  try_run(program_result
          compilation_succeeded
          "${CMAKE_BINARY_DIR}"
          "${CMAKE_CURRENT_SOURCE_DIR}/cmake/get_compiler_version.cpp")
  set(CMAKE_CXX_FLAGS "${CXXFLAGS_BACKUP}")
  if(NOT compilation_succeeded)
    message(STATUS "Address Sanitizer not available on selected compiler")
  else()
    message(STATUS "Enabling Address Sanitizer")
    set(ASAN_FOUND true)
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
  endif()
endif ()

# set -fno-exception if requested
if (NO_EXCEPTIONS)
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -fno-exceptions")
endif()

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
  endif()
  # Create a CMake script that removes all *.gcda files. We'll call this below
  # in the custom target.
  file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/remove_gcda.cmake
    "file(GLOB_RECURSE GCDA_FILES \"${CMAKE_CURRENT_BINARY_DIR}/*.gcda\")\n"
    "if(NOT GCDA_FILES STREQUAL \"\")\n"
    "  file(REMOVE \${GCDA_FILES})\n"
    "endif()")
  file(GLOB_RECURSE GCOV_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/libvast/*")
  set(GCOV_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/coverage")
  # Add new target that invokes the unit test suite followed by gcov.
  add_custom_target(gcov
    # Reset coverage counters.
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/remove_gcda.cmake
    # Run unit tests.
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    # Invoke gcov.
    COMMAND ${CMAKE_COMMAND}
      -DGCOV_SOURCES="${GCOV_SOURCES}"
      -DGCOV_OUTPUT_DIR="${GCOV_OUTPUT_DIR}"
      -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/GenerateGcov.cmake
    COMMENT "Generating gcov output")
  # Add gcovr targets to generate various output files.
  add_custom_target(gcovr-txt
    COMMAND gcovr -k -g -r .. -e libvast/test
              --object-directory=${GCOV_OUTPUT_DIR}
              -o coverage.txt
    COMMENT "Creating gcov text summary")
  add_custom_target(gcovr-xml
    COMMAND gcovr -k -g -r .. -e libvast/test
              --object-directory=${GCOV_OUTPUT_DIR}
              -x -o coverage.xml
    COMMENT "Creating gcov XML summary")
  add_custom_target(gcovr-html
    COMMAND gcovr -k -g -r .. -e libvast/test
              --object-directory=${GCOV_OUTPUT_DIR}
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

function(check_working_cxx_atomics128 varname)
set(OLD_CMAKE_REQUIRED_FLAGS ${CMAKE_REQUIRED_FLAGS})
set(CMAKE_REQUIRED_FLAGS "-std=c++11 ${CMAKE_REQUIRED_FLAGS}")
CHECK_CXX_SOURCE_COMPILES("
#include <atomic>
#include <cstdint>
struct sixteen {
  uint64_t x = 1;
  int64_t  y = -1;
};
std::atomic<sixteen> x;
int main()
{
  auto v = x.load(std::memory_order_relaxed);
  return std::atomic_is_lock_free(&x);
}
" ${varname})
set(CMAKE_REQUIRED_FLAGS ${OLD_CMAKE_REQUIRED_FLAGS})
endfunction(check_working_cxx_atomics128)

check_working_cxx_atomics128(HAVE_CXX_ATOMICS_WITHOUT_LIB)
if (NOT HAVE_CXX_ATOMICS_WITHOUT_LIB)
  check_library_exists(atomic __atomic_fetch_add_16 "" HAVE_LIBATOMIC)
  if (HAVE_LIBATOMIC)
    list(APPEND CMAKE_REQUIRED_LIBRARIES "atomic")
    check_working_cxx_atomics128(HAVE_CXX_ATOMICS_WITH_LIB)
    if (NOT HAVE_CXX_ATOMICS_WITH_LIB)
      message(FATAL_ERROR "Host compiler must support std::atomic!")
    endif()
  else()
    message(STATUS "Host compiler appears to require libatomic, but cannot find it")
    message(STATUS "Enabling mutex lock workaround")
    set(VAST_MEASUREMENT_MUTEX_WORKAROUND ON)
  endif()
endif ()

# Only set our flags if the environment variable CXXFLAGS is not present.
if (NOT CMAKE_CXX_FLAGS)
  set(CMAKE_CXX_FLAGS ${EXTRA_FLAGS})
endif ()

if (LDFLAGS)
  set(CMAKE_EXE_LINKER_FLAGS ${LDFLAGS})
  set(CMAKE_SHARED_LINKER_FLAGS ${LDFLAGS})
  set(CMAKE_STATIC_LINKER_FLAGS ${LDFLAGS})
endif()

#------------------------------------------------------------------------------
#                          Dependency Configuration
#------------------------------------------------------------------------------

find_package(Threads)
if (NOT Threads_FOUND)
  message(FATAL_ERROR "Could not find system threading libraries")
endif ()


if (CMAKE_BUILD_TYPE STREQUAL "Release")
  if (VAST_ENABLE_ASSERTIONS)
    message(STATUS "Automatically disabling assertions for Release build")
    set (VAST_ENABLE_ASSERTIONS false)
  endif ()
endif ()

if (VAST_ENABLE_ASSERTIONS)
  find_package(Backtrace)
  if (NOT Backtrace_FOUND)
    message(FATAL_ERROR "Could not find backtrace (execinfo) libraries")
  endif ()
endif ()

if (NOT CAF_ROOT_DIR AND VAST_PREFIX)
  set(CAF_ROOT_DIR ${VAST_PREFIX})
endif ()
# Try to find the required CAF components first...
find_package(CAF COMPONENTS core io test QUIET)
if (NOT CAF_FOUND)
  message(FATAL_ERROR "Could not find CAF")
endif ()
message(STATUS "Found CAF")
set(CAF_FOUND_SAVE ${CAF_FOUND})
# ...and then optional components.
find_package(CAF COMPONENTS openssl QUIET)
if (CAF_LIBRARY_OPENSSL)
  message(STATUS "Found CAF's OpenSSL module, enabling OpenSSL support")
  set(VAST_USE_OPENSSL true)
endif ()
set(CAF_FOUND ${CAF_FOUND_SAVE})

if (BROKER_ROOT_DIR)
  find_package(BROKER REQUIRED)
  set(VAST_HAVE_BROKER true)
else ()
  if (VAST_PREFIX)
    set(BROKER_ROOT_DIR "${VAST_PREFIX}")
  endif ()
  find_package(BROKER QUIET)
  if (BROKER_FOUND)
    set(VAST_HAVE_BROKER true)
  endif ()
endif ()

if (NOT SNAPPY_ROOT_DIR AND VAST_PREFIX)
  set(SNAPPY_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(SNAPPY QUIET)
if (SNAPPY_FOUND)
  set(VAST_HAVE_SNAPPY true)
endif ()

if (NOT PCAP_ROOT_DIR AND VAST_PREFIX)
  set(PCAP_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(PCAP QUIET)
if (PCAP_FOUND)
  set(VAST_HAVE_PCAP true)
endif ()

if (NOT Gperftools_ROOT_DIR AND VAST_PREFIX)
  set(Gperftools_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(Gperftools QUIET)
if (GPERFTOOLS_FOUND)
  set(VAST_HAVE_GPERFTOOLS true)
endif ()

find_package(Doxygen QUIET)
find_package(Md2man QUIET)

#------------------------------------------------------------------------------
#                                   Docker
#------------------------------------------------------------------------------

set(docker_image "mavam/vast")
set(docker_image_ver "${docker_image}:${VAST_VERSION}")

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/docker-build.cmake
  "execute_process(
     COMMAND docker build -t ${docker_image_ver} ${CMAKE_SOURCE_DIR}
     RESULT_VARIABLE docker_build_retval)
   if (NOT \"\${docker_build_retval}\" EQUAL 0)
     message(FATAL_ERROR \"Failed building the docker image.\")
   endif ()
   execute_process(
     COMMAND docker tag ${docker_image_ver} ${docker_image}:latest)")

add_custom_target(docker-build
  ${CMAKE_COMMAND} -P ${CMAKE_BINARY_DIR}/docker-build.cmake
  COMMENT "Building docker image")

add_custom_target(docker-run
  COMMAND docker run -ti ${docker_image}
  COMMENT "Running current Docker image" VERBATIM)

#------------------------------------------------------------------------------
#                                 Build Setup
#------------------------------------------------------------------------------

# make uninstall
if ("${PROJECT_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
  configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
    @ONLY)
  add_custom_target(uninstall COMMAND
    ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
endif ()

# make configure
add_custom_target(configure COMMAND ${CMAKE_CURRENT_BINARY_DIR}/config.status)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/bin
  CACHE PATH "Single directory for all executables")

set (LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_BINARY_DIR}/lib CACHE
  PATH "Single directory for all libraries")

file(GENERATE
  OUTPUT
    "${CMAKE_CURRENT_BINARY_DIR}/integration_$<CONFIG>.sh"
  CONTENT
    "#!/bin/sh
     base_dir=\"${CMAKE_CURRENT_SOURCE_DIR}/scripts/integration\"
     env_dir=\"${CMAKE_CURRENT_BINARY_DIR}/integration_env\"
     app=\"${CMAKE_CURRENT_BINARY_DIR}/bin/vast\"
     set -e
     if [ ! -d \"$env_dir\" ]; then
       python3 -m venv \"$env_dir\"
     fi
     source \"$env_dir/bin/activate\"
     pip install -r \"$base_dir/requirements.txt\"
     python \"$base_dir/integration.py\" --app \"$app\" -d vast-integration-test")

add_custom_target(integration
  COMMAND /bin/sh "${CMAKE_CURRENT_BINARY_DIR}/integration_$<CONFIG>.sh")


add_subdirectory(doc)
add_subdirectory(libvast)
add_subdirectory(libvast_test)
add_subdirectory(tools)
add_subdirectory(vast)

#------------------------------------------------------------------------------
#                           Additional Installations
#------------------------------------------------------------------------------

if (VAST_RELOCATEABLE_INSTALL AND NOT BUILD_SHARED_LIBS)
  # Copy CAF libraries to installation directory
  get_filename_component(CAF_LIBDIR ${CAF_LIBRARY_CORE} PATH)
  install(DIRECTORY "${CAF_LIBDIR}/" DESTINATION "${CMAKE_INSTALL_LIBDIR}/")
endif ()

export(EXPORT VASTTargets
  FILE VASTTargets.cmake
  NAMESPACE vast::
)

install(EXPORT VASTTargets
  DESTINATION "${INSTALL_VAST_CMAKEDIR}"
  NAMESPACE vast::
)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/VASTConfigVersion.cmake"
  VERSION ${VAST_VERSION}
  COMPATIBILITY ExactVersion
)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/VASTConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/VASTConfig.cmake"
  INSTALL_DESTINATION "${INSTALL_VAST_CMAKEDIR}"
)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/VASTConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/VASTConfigVersion.cmake"
  DESTINATION "${INSTALL_VAST_CMAKEDIR}"
)

install(DIRECTORY
  "${CMAKE_CURRENT_SOURCE_DIR}/scripts/integration"
  DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/"
)

install(FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/vast.conf"
  DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/"
  RENAME "default.conf"
)
install(FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/vast.conf"
  DESTINATION "${CMAKE_INSTALL_SYSCONFDIR}/vast/"
)

#------------------------------------------------------------------------------
#                                Build Summary
#------------------------------------------------------------------------------

macro(display test desc summary)
  if (${test})
    set(${summary} ${desc})
  else ()
    set(${summary} no)
  endif()
endmacro()

display(SHOW_TIME_REPORT yes time_report_summary)
display(VAST_ENABLE_ASSERTIONS yes assertions_summary)
display(ASAN_FOUND yes asan_summary)
display(ENABLE_GCOV yes gcov_summary)

# Figure out whether we point to a build directory or a prefix.
set(caf_dir ${CAF_LIBRARY_CORE})
get_filename_component(caf_dir ${caf_dir} PATH)
if (EXISTS "${caf_dir}/../libcaf_core")
  get_filename_component(caf_dir ${caf_dir} PATH)
else ()
  set(caf_dir ${CAF_INCLUDE_DIR_CORE})
endif ()

# Figure out whether we point to a build directory or a prefix.
if (BROKER_FOUND)
  set(broker_dir ${BROKER_LIBRARY})
  get_filename_component(broker_dir ${broker_dir} PATH)
  if (EXISTS "${broker_dir}/../broker")
    get_filename_component(broker_dir ${broker_dir} PATH)
  else ()
    set(broker_dir ${BROKER_INCLUDE_DIRS})
  endif ()
endif ()

display(CAF_FOUND "${caf_dir}" caf_summary)
display(BROKER_FOUND "${broker_dir}" broker_summary)
display(SNAPPY_FOUND "${SNAPPY_INCLUDE_DIR}" snappy_summary)
display(PCAP_FOUND "${PCAP_INCLUDE_DIR}" pcap_summary)
display(GPERFTOOLS_FOUND "${GPERFTOOLS_INCLUDE_DIR}" perftools_summary)
display(DOXYGEN_FOUND yes doxygen_summary)
display(MD2MAN_FOUND yes md2man_summary)
display(VAST_USE_TCMALLOC yes tcmalloc_summary)
display(VAST_USE_OPENSSL yes openssl_summary)
display(NO_UNIT_TESTS no build_tests_summary)
display(VAST_RELOCATEABLE_INSTALL yes relocatable_install_summary)

STRING(TOUPPER "${CMAKE_BUILD_TYPE}" build_type)
set(summary
    "====================|  Build Summary  |===================="
    "\n"
    "\nVersion:             ${VERSION_MAJ_MIN}"
    "\n"
    "\nSource directory:    ${CMAKE_SOURCE_DIR}"
    "\nBuild directory:     ${CMAKE_BINARY_DIR}"
    "\nInstall prefix:      ${CMAKE_INSTALL_PREFIX}"
    "\n"
    "\nBuild type:          ${CMAKE_BUILD_TYPE}"
    "\nBuild unit tests:    ${build_tests_summary}"
    "\nLog level:           ${VAST_LOG_LEVEL}"
    "\nShow time report:    ${time_report_summary}"
    "\nAssertions:          ${assertions_summary}"
    "\nAddressSanitizer:    ${asan_summary}"
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
    "\nCAF:                 ${caf_summary}"
    "\nBroker:              ${broker_summary}"
    "\nSnappy               ${snappy_summary}"
    "\nPCAP:                ${pcap_summary}"
    "\nGperftools:          ${perftools_summary}"
    "\nDoxygen:             ${doxygen_summary}"
    "\nmd2man:              ${md2man_summary}"
    "\n"
    "\ntcmalloc:            ${tcmalloc_summary}"
    "\nOpenSSL:             ${openssl_summary}"
    "\n"
    "\nRelocatable install: ${relocatable_install_summary}"
    "\n"
    "\n===========================================================")

message("\n" ${summary} "\n")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/config.summary ${summary})
