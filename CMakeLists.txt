#------------------------------------------------------------------------------
#                                Project Setup
#------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.0 FATAL_ERROR)
project(VAST C CXX)

# Silence warning CMP0042
if (APPLE AND NOT DEFINED CMAKE_MACOSX_RPATH)
  set(CMAKE_MACOSX_RPATH true)
endif()

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(CTest)
include(Helpers)
include(MacDependencyPaths)

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
#                               Compiler Setup
#------------------------------------------------------------------------------

# Ninja doesn't colorize compiler diagnostics by default.
if (CMAKE_GENERATOR STREQUAL "Ninja")
  set(EXTRA_FLAGS "-fdiagnostics-color")
endif ()

# To give the user full control, we don't mess with with CXX_FLAGS if provided.
# This is a deliberate decision but contrasts to many other packages that
# append flags to the user-provide environment variable.
if (NOT CMAKE_CXX_FLAGS)
  # Our default flags.
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -std=c++14 -Wall -Wextra -pedantic")
  set(PERFORMANCE_FLAGS "-march=native -mpopcnt")
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

# Requirement checks
try_run(program_result
        compilation_succeeded
        "${CMAKE_CURRENT_BINARY_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/get_compiler_version.cpp"
        RUN_OUTPUT_VARIABLE compiler_version)
if (NOT compilation_succeeded OR NOT program_result EQUAL 0)
  message(FATAL_ERROR "Cannot determine compiler version")
elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  # Too many false positives.
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -Wno-maybe-uninitialized")
  if (compiler_version VERSION_GREATER 6.0
      OR compiler_version VERSION_EQUAL 6.0)
    message(STATUS "Found GCC ${compiler_version}")
  else ()
    message(FATAL_ERROR "GCC >= 6.0 required, found ${compiler_version}")
  endif ()
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
  if (compiler_version VERSION_GREATER 3.5
      OR compiler_version VERSION_EQUAL 3.5)
    message(STATUS "Found Clang ${compiler_version}")
  else ()
    message(FATAL_ERROR "Clang >= 3.5 required, found ${compiler_version}")
  endif ()
  if (NOT NO_AUTO_LIBCPP)
    # Standard library selection: check if libc++ is available, and if so, use
    # it. Otherwise fall back to libstdc++.
    set(CXXFLAGS_BACKUP "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "-std=c++14 -stdlib=libc++")
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
    set(EXTRA_FLAGS "${EXTRA_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
  endif()
endif ()

if (ENABLE_GCOV)
  # Locate gcov and gcovr.
  find_package(GCOV)
  if (NOT GCOV_FOUND)
    message(FATAL_ERROR "Could not find gcov")
  endif ()
  find_package(GCOV)
  if (NOT GCOV_FOUND)
    message(FATAL_ERROR "Could not find gcovr")
  endif ()
  # For compile flags, GCC and Clang share the same options.
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -fprofile-arcs -ftest-coverage")
  set(EXTRA_FLAGS "${EXTRA_FLAGS} -fno-omit-frame-pointer")
  # For link flags, GCC wants -lgcov and Clang --coverage.
  if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
    set(LDFLAGS ${LDFLAGS} -lgcov)
  elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    set(LDFLAGS ${LDFLAGS} --coverage)
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

# Only set our flags if the environment variable CXXFLAGS is not present.
if (NOT CMAKE_CXX_FLAGS)
  set(CMAKE_CXX_FLAGS ${EXTRA_FLAGS})
endif ()

if (LDFLAGS)
  set(CMAKE_EXE_LINKER_FLAGS ${LDFLAGS})
  set(CMAKE_SHARED_LINKER_FLAGS ${LDFLAGS})
  set(CMAKE_STATIC_LINKER_FLAGS ${LDFLAGS})
endif()

# Pick up 3rd-party packages and ports.
if (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
  include_directories(/usr/local/include)
  link_directories(/usr/local/lib)
endif ()

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
    include_directories(${Backtrace_INCLUDE_DIRS})
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
include_directories(${CAF_INCLUDE_DIRS})
# ...and then optional components.
find_package(CAF COMPONENTS opencl openssl QUIET)
if (CAF_LIBRARY_OPENCL)
  message(STATUS "Found CAF's OpenCL module, enabling OpenCL support")
  include_directories(${CAF_INCLUDE_DIR_OPENCL})
  find_package(OpenCL)
  if (NOT OpenCL_FOUND)
    message(FATAL_ERROR
            "Could not find OpenCL despite having found CAF' OpenCL module")
  endif ()
  include_directories(${OpenCL_INCLUDE_DIRS})
  set(VAST_USE_OPENCL true)
endif ()
if (CAF_LIBRARY_OPENSSL)
  message(STATUS "Found CAF's OpenSSL module, enabling OpenSSL support")
  include_directories(${CAF_INCLUDE_DIR_OPENSSL})
  set(VAST_USE_OPENSSL true)
endif ()
set(CAF_FOUND ${CAF_FOUND_SAVE})

if (NOT SNAPPY_ROOT_DIR AND VAST_PREFIX)
  set(SNAPPY_ROOT_DIR ${VAST_PREFIX})
endif ()
find_package(SNAPPY QUIET)
if (SNAPPY_FOUND)
  set(VAST_HAVE_SNAPPY true)
  include_directories(${SNAPPY_INCLUDE_DIR})
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

#------------------------------------------------------------------------------
#                                   Docker
#------------------------------------------------------------------------------

set(docker_image "mavam/vast")

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/docker-build.cmake
  "execute_process(
     COMMAND docker build -t ${docker_image} ${CMAKE_SOURCE_DIR}
     RESULT_VARIABLE docker_build_success)
   set(docker_build_success true)
   if (docker_build_success)
     execute_process(
       COMMAND docker inspect --format='{{.Id}}' ${docker_image}
       OUTPUT_VARIABLE docker_image_id)
     execute_process(
       COMMAND docker tag \${docker_image_id} ${docker_image}:latest
       COMMAND docker tag \${docker_image_id} ${docker_image}:${VAST_VERSION})
   endif ()")

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

include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR})
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/aux)
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/aux/date/include)

add_subdirectory(doc)
add_subdirectory(libvast)
add_subdirectory(tools)
add_subdirectory(vast)

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
display(ENABLE_ADDRESS_SANITIZER yes asan_summary)
display(ASAN_FOUND yes asan_summary)
display(ENABLE_GCOV yes gcov_summary)

if (VAST_LOG_LEVEL EQUAL -1)
  set(log_level_summary quiet)
elseif (VAST_LOG_LEVEL EQUAL 0)
  set(log_level_summary error)
elseif (VAST_LOG_LEVEL EQUAL 1)
  set(log_level_summary warning)
elseif (VAST_LOG_LEVEL EQUAL 2)
  set(log_level_summary info)
elseif (VAST_LOG_LEVEL EQUAL 3)
  set(log_level_summary debug)
elseif (VAST_LOG_LEVEL EQUAL 4)
  set(log_level_summary trace)
else ()
  message(FATAL_ERROR "Invalid log level: ${VAST_LOG_LEVEL}")
endif ()

set(caf_dir ${CAF_LIBRARY_CORE})
get_filename_component(caf_dir ${caf_dir} PATH)
if (EXISTS "${caf_dir}/../libcaf_core")
  get_filename_component(caf_dir ${caf_dir} PATH)
else ()
  set(caf_dir ${CAF_INCLUDE_DIR_CORE})
endif ()

display(CAF_FOUND ${caf_dir} caf_summary)
display(SNAPPY_FOUND "${SNAPPY_INCLUDE_DIR}" snappy_summary)
display(PCAP_FOUND "${PCAP_INCLUDE_DIR}" pcap_summary)
display(GPERFTOOLS_FOUND "${GPERFTOOLS_INCLUDE_DIR}" perftools_summary)
display(DOXYGEN_FOUND yes doxygen_summary)
display(MD2MAN_FOUND yes md2man_summary)
display(VAST_USE_TCMALLOC yes tcmalloc_summary)
display(VAST_USE_OPENCL "${OpenCL_LIBRARIES}" opencl_summary)
display(VAST_USE_OPENSSL yes openssl_summary)

STRING(TOUPPER "${CMAKE_BUILD_TYPE}" build_type)
set(summary
    "====================|  Build Summary  |===================="
    "\n"
    "\nVersion:          ${VERSION_MAJ_MIN}"
    "\n"
    "\nSource directory: ${CMAKE_SOURCE_DIR}"
    "\nBuild directory:  ${CMAKE_BINARY_DIR}"
    "\nInstall prefix:   ${CMAKE_INSTALL_PREFIX}"
    "\n"
    "\nBuild type:       ${CMAKE_BUILD_TYPE}"
    "\nLog level:        ${log_level_summary}"
    "\nShow time report: ${time_report_summary}"
    "\nAssertions:       ${assertions_summary}"
    "\nAddressSanitizer: ${asan_summary}"
    "\ngcov:             ${gcov_summary}"
    "\n"
    "\nCC:               ${CMAKE_C_COMPILER}"
    "\nCXX:              ${CMAKE_CXX_COMPILER}"
    "\nCXXFLAGS:         ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${build_type}}"
    "\nLDFLAGS:          ${LDFLAGS}"
    "\n"
    "\nCAF:              ${caf_summary}"
    "\nSnappy            ${snappy_summary}"
    "\nPCAP:             ${pcap_summary}"
    "\nGperftools:       ${perftools_summary}"
    "\nDoxygen:          ${doxygen_summary}"
    "\nmd2man:           ${md2man_summary}"
    "\n"
    "\ntcmalloc:         ${tcmalloc_summary}"
    "\nOpenCL:           ${opencl_summary}"
    "\nOpenSSL:          ${openssl_summary}"
    "\n"
    "\n===========================================================")

message("\n" ${summary} "\n")
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/config.summary ${summary})
