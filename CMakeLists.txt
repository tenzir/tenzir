cmake_minimum_required(VERSION 3.18...3.21 FATAL_ERROR)

# -- project setup -------------------------------------------------------------

# Semicolon-separated list of directories specifying a search path for CMake.
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set(VAST_VERSION_TAG_BACKUP "${VAST_VERSION_TAG}")
include(VASTVersion)

# Create the VAST project.
project(
  VAST
  VERSION
    ${VAST_VERSION_MAJOR}.${VAST_VERSION_MINOR}.${VAST_VERSION_PATCH}.${VAST_VERSION_TWEAK}
  DESCRIPTION "Visibility Across Space and Time"
  HOMEPAGE_URL "https://github.com/tenzir/vast"
  LANGUAGES C CXX)

# -- sanity checks -------------------------------------------------------------

# Prohibit in-source builds.
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif ()

# Ensure that CMAKE_INSTALL_PREFIX is not a relative path.
if (NOT IS_ABSOLUTE "${CMAKE_INSTALL_PREFIX}")
  message(
    FATAL_ERROR
      "CMAKE_INSTALL_PREFIX must be an absolute path: ${CMAKE_INSTALL_PREFIX}")
endif ()

# Override `find_package` for subprojects to be a no-op when trying to find VAST
# itself. This simplifies the plugin CMake by not requiring plugin developers to
# check whether their plugin is built alongside VAST.
if ("${CMAKE_PROJECT_NAME}" STREQUAL "${PROJECT_NAME}")
  macro (find_package)
    if (NOT "${ARGV0}" STREQUAL "${CMAKE_PROJECT_NAME}")
      _find_package(${ARGV})
    endif ()
  endmacro ()
endif ()

# Set a default build type if none was specified.
set(default_build_type "Release")
if (EXISTS "${CMAKE_SOURCE_DIR}/.git")
  set(default_build_type "Debug")
endif ()

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(
    STATUS
      "Setting build type to '${default_build_type}' as none was specified.")
  set(CMAKE_BUILD_TYPE
      "${default_build_type}"
      CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui.
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
                                               "MinSizeRel" "RelWithDebInfo")
endif ()

# -- includes ------------------------------------------------------------------

include(CMakeDependentOption)
include(CMakePackageConfigHelpers)
include(CMakePrintHelpers)
include(CPack)
include(CTest)
include(CheckCXXSourceCompiles)
include(CheckLibraryExists)
include(FeatureSummary)
include(GNUInstallDirs)
include(VASTCheckCompilerVersion)
include(VASTMacDependencyPaths)
include(VASTProvideFindModule)
include(VASTRegisterPlugin)
include(VASTUtilities)

# -- project configuration -----------------------------------------------------

# Determine whether we're building VAST a subproject. This is used to determine
# good default values for many options. VAST should not modify global CMake if
# built as a subproject, unless explicitly requested to do so with options.
get_directory_property(_VAST_PARENT_DIRECTORY PARENT_DIRECTORY)
if (_VAST_PARENT_DIRECTORY)
  set(VAST_IS_SUBPROJECT ON)
  set(VAST_IS_NOT_SUBPROJECT OFF)
else ()
  set(VAST_IS_SUBPROJECT OFF)
  set(VAST_IS_NOT_SUBPROJECT ON)
endif ()
unset(_VAST_PARENT_DIRECTORY)

# -- changelog -----------------------------------------------------------------

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/VASTChangelog.cmake.in"
               "${CMAKE_CURRENT_BINARY_DIR}/VASTChangelog.cmake" @ONLY)

file(GLOB_RECURSE changelog_dependencies CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/changelog/**.md")

add_custom_target(
  changelog ALL
  BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/CHANGELOG.md"
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/VASTChangelog.cmake"
          ${changelog_dependencies}
  COMMAND ${CMAKE_COMMAND} -P "${CMAKE_CURRENT_BINARY_DIR}/VASTChangelog.cmake")

# NOTE: This does intentionally not specify <source-dir>/CHANGELOG.md as a
# byproduct, because that causes the generated 'clean' target to remove it.
add_custom_target(
  update-changelog
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/CHANGELOG.md"
  COMMAND
    ${CMAKE_COMMAND} -E copy_if_different
    "${CMAKE_CURRENT_BINARY_DIR}/CHANGELOG.md"
    "${CMAKE_CURRENT_SOURCE_DIR}/CHANGELOG.md")

option(VAST_ENABLE_SKIP_AFTER_CHANGELOG_UPDATE
       "Skip build configuration after update-changelog target" OFF)
mark_as_advanced(VAST_ENABLE_SKIP_AFTER_CHANGELOG_UPDATE)
if (VAST_ENABLE_SKIP_AFTER_CHANGELOG_UPDATE)
  return()
endif ()

# -- testing -------------------------------------------------------------------

cmake_dependent_option(VAST_ENABLE_UNIT_TESTS "Build unit tests for libvast" ON
                       "NOT CMAKE_CROSS_COMPILING" OFF)
add_feature_info("VAST_ENABLE_UNIT_TESTS" VAST_ENABLE_UNIT_TESTS
                 "build unit tests for libvast.")

# -- library flavor ------------------------------------------------------------

option(BUILD_SHARED_LIBS "Build shared instead of static libraries" ON)
add_feature_info("BUILD_SHARED_LIBS" BUILD_SHARED_LIBS
                 "build shared instead of static libraries.\n")

# -- relocatable installations -------------------------------------------------

# Setting this option removes configuration dependent absolute paths from the
# VAST installation. Concretely, it enables the dynamic binary and libraries to
# use relative paths for loading their dependencies. On macOS, always enable
# relocatable installations as it's very uncommon not to do so.
cmake_dependent_option(VAST_ENABLE_RELOCATABLE_INSTALLATIONS
                       "Enable relocatable installations" ON "NOT APPLE" ON)
add_feature_info(
  "VAST_ENABLE_RELOCATABLE_INSTALLATIONS" VAST_ENABLE_RELOCATABLE_INSTALLATIONS
  "enable relocatable installations.")

if (VAST_ENABLE_RELOCATABLE_INSTALLATIONS)
  # For relocatable builds we need the configured install directories to be
  # relative so we can interpret them relative to the object path of the running
  # binary instead of a fixed install prefix.
  VASTNormalizeInstallDirs()

  # Single output directory.
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
      "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_INSTALL_BINDIR}"
      CACHE PATH "Single directory for all binaries.")
  set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
      "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR}"
      CACHE PATH "Single directory for all libraries.")
endif ()

# CMake allows for overriding the install prefix at install-time, e.g., via
# `cmake --install <path/to/build> --prefix <path/to/install>` or
# `DESTDIR=<path/to/install> cmake --build <path/to/build> --target install`.
# Doing so breaks non-relocatable installations, so we abort the installation
# when encountering this.
if (NOT VAST_ENABLE_RELOCATABLE_INSTALLATIONS)
  install(
    CODE "\
    get_filename_component(build_time_prefix
      \"${CMAKE_INSTALL_PREFIX}\" ABSOLUTE)
    get_filename_component(install_time_prefix
      \"\${CMAKE_INSTALL_PREFIX}\" ABSOLUTE)
    if (NOT \"\${build_time_prefix}\" STREQUAL \"\${install_time_prefix}\")
      message(FATAL_ERROR
        \"For non-relocatable builds the configured build-time install prefix (\${build_time_prefix}) must match the configured install-time install prefix (\${install_time_prefix})\"
      )
    endif ()
  ")
endif ()

# -- silence policy-related warnings -------------------------------------------

# TODO: When updating to CAF 0.18, this should no longer be needed.
if (POLICY CMP0048)
  unset(PROJECT_VERSION)
  unset(PROJECT_VERSION_MAJOR)
  unset(PROJECT_VERSION_MINOR)
  unset(PROJECT_VERSION_PATCH)
  unset(PROJECT_VERSION_TWEAK)
endif ()

# -- internal target -----------------------------------------------------------

# Create the internal target that is used for option/property propagation.
add_library(libvast_internal INTERFACE)
set_target_properties(libvast_internal PROPERTIES EXPORT_NAME internal)
add_library(vast::internal ALIAS libvast_internal)
install(TARGETS libvast_internal EXPORT VASTTargets)

# Require standard C++20.
target_compile_features(libvast_internal INTERFACE cxx_std_20)

# Increase maximum number of template instantiations.
target_compile_options(libvast_internal INTERFACE -ftemplate-backtrace-limit=0)

# -- FreeBSD support -----------------------------------------------------------

if (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
  # Works around issues with libstdc++ and C++11. For details, see: -
  # https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=194929 -
  # https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=182657
  target_compile_definitions(
    libvast_internal INTERFACE _GLIBCXX_USE_C99 _GLIBCXX_USE_C99_MATH
                               _GLIBCXX_USE_C99_MATH_TR1)
endif ()

# -- optimizations -------------------------------------------------------------

option(VAST_ENABLE_AUTO_VECTORIZATION "Enable SSE instructions" ON)
add_feature_info(
  "VAST_ENABLE_AUTO_VECTORIZATION" VAST_ENABLE_AUTO_VECTORIZATION
  "enable auto-vectorization via supported SSE/AVX instructions.")
if (VAST_ENABLE_AUTO_VECTORIZATION)
  find_package(SSE QUIET)
  option(VAST_ENABLE_SSE_INSTRUCTIONS "Enable SSE instructions" "${SSE_FOUND}")
  add_feature_info("VAST_ENABLE_SSE_INSTRUCTIONS" VAST_ENABLE_SSE_INSTRUCTIONS
                   "enable SSE instructions.")
  option(VAST_ENABLE_SSE2_INSTRUCTIONS "Enable SSE2 instructions"
         "${SSE2_FOUND}")
  add_feature_info("VAST_ENABLE_SSE2_INSTRUCTIONS"
                   VAST_ENABLE_SSE2_INSTRUCTIONS "enable SSE2 instructions.")
  option(VAST_ENABLE_SSE3_INSTRUCTIONS "Enable SSE3 instructions"
         "${SSE3_FOUND}")
  add_feature_info("VAST_ENABLE_SSE3_INSTRUCTIONS"
                   VAST_ENABLE_SSE3_INSTRUCTIONS "enable SSE3 instructions.")
  option(VAST_ENABLE_SSSE3_INSTRUCTIONS "Enable SSSE3 instructions"
         "${SSSE3_FOUND}")
  add_feature_info("VAST_ENABLE_SSSE3_INSTRUCTIONS"
                   VAST_ENABLE_SSSE3_INSTRUCTIONS "enable SSSE3 instructions.")
  option(VAST_ENABLE_SSE4_1_INSTRUCTIONS "Enable SSE4.1 instructions"
         "${SSE4_1_FOUND}")
  add_feature_info(
    "VAST_ENABLE_SSE4_1_INSTRUCTIONS" VAST_ENABLE_SSE4_1_INSTRUCTIONS
    "enable SSE4.1 instructions.")
  option(VAST_ENABLE_SSE4_2_INSTRUCTIONS "Enable SSE4.2 instructions"
         "${SSE4_2_FOUND}")
  add_feature_info(
    "VAST_ENABLE_SSE4_2_INSTRUCTIONS" VAST_ENABLE_SSE4_2_INSTRUCTIONS
    "enable SSE4.2 instructions.")
  option(VAST_ENABLE_AVX_INSTRUCTIONS "Enable AVX instructions" "${AVX_FOUND}")
  add_feature_info("VAST_ENABLE_AVX_INSTRUCTIONS" VAST_ENABLE_AVX_INSTRUCTIONS
                   "enable AVX instructions.")
  option(VAST_ENABLE_AVX2_INSTRUCTIONS "Enable AVX2 instructions"
         "${AVX2_FOUND}")
  add_feature_info("VAST_ENABLE_AVX2_INSTRUCTIONS"
                   VAST_ENABLE_AVX2_INSTRUCTIONS "enable AVX2 instructions.")
endif ()

target_compile_options(
  libvast_internal
  INTERFACE $<$<CONFIG:Release>:
            -fno-omit-frame-pointer>
            $<$<CONFIG:RelWithDebInfo>:-fno-omit-frame-pointer>
            $<$<CONFIG:Debug>:-O0>
            $<$<CONFIG:CI>:-O2
            -g1>
            $<$<BOOL:${VAST_ENABLE_SSE_INSTRUCTIONS}>:-msse>
            $<$<BOOL:${VAST_ENABLE_SSE2_INSTRUCTIONS}>:-msse2>
            $<$<BOOL:${VAST_ENABLE_SSE3_INSTRUCTIONS}>:-msse3>
            $<$<BOOL:${VAST_ENABLE_SSSE3_INSTRUCTIONS}>:-mssse3>
            $<$<BOOL:${VAST_ENABLE_SSE4_1_INSTRUCTIONS}>:-msse4.1>
            $<$<BOOL:${VAST_ENABLE_SSE4_2_INSTRUCTIONS}>:-msse4.2>
            $<$<BOOL:${VAST_ENABLE_AVX_INSTRUCTIONS}>:-mavx>
            $<$<BOOL:${VAST_ENABLE_AVX2_INSTRUCTIONS}>:-mavx2>)

# -- warnings ------------------------------------------------------------------

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(libvast_internal INTERFACE -Wno-unknown-warning-option)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  target_compile_options(libvast_internal INTERFACE -Wno-unknown-warning)
endif ()

target_compile_options(
  libvast_internal
  INTERFACE -Wall
            -Wextra
            -pedantic
            -Werror=switch
            -Wundef
            $<$<CONFIG:CI>:-Werror
            # Never treat deprecation warnings as errors.
            -Wno-error=deprecated
            -Wno-error=deprecated-declarations>
            $<$<CXX_COMPILER_ID:GNU>:-Wno-redundant-move>
            $<$<CXX_COMPILER_ID:GNU>:-Wno-error=bool-compare>)

# -- build id ------------------------------------------------------------------

cmake_dependent_option(
  VAST_ENABLE_BUILDID "Include a unique identifier in the elf notes section" ON
  "CMAKE_EXECUTABLE_FORMAT STREQUAL ELF" OFF)
add_feature_info("VAST_ENABLE_BUILDID" VAST_ENABLE_BUILDID
                 "a unique identifier in the ELF notes section.")
if (VAST_ENABLE_BUILDID)
  target_link_options(libvast_internal INTERFACE "-Wl,--build-id")
endif ()

# -- USDT tracepoints ----------------------------------------------------------

option(VAST_ENABLE_SDT "Generate USDT tracepoint instrumentation" ON)
add_feature_info("VAST_ENABLE_SDT" VAST_ENABLE_SDT
                 "generate USDT tracepoint instrumentation.")

# -- build profiling -----------------------------------------------------------

option(VAST_ENABLE_TIME_REPORT
       "Print information where time was spent during compilation" OFF)
add_feature_info("VAST_ENABLE_TIME_REPORT" VAST_ENABLE_TIME_REPORT
                 "print information where time was spent during compilation.")
if (VAST_ENABLE_TIME_REPORT)
  target_compile_options(libvast_internal INTERFACE "-ftime-report")
endif ()

option(VAST_ENABLE_TIME_TRACE
       "Generate tracing JSON for compilation time profiling" OFF)
add_feature_info("VAST_ENABLE_TIME_TRACE" VAST_ENABLE_TIME_TRACE
                 "generate tracing JSON for compilation time profiling.")
if (VAST_ENABLE_TIME_TRACE)
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag("-ftime-trace" _time_trace_supported)
  if (NOT _time_trace_supported)
    message(
      FATAL_ERROR
        "-ftime-trace option not supported by compiler ${CMAKE_CXX_COMPILER}")
  endif ()
  unset(_time_trace_supported)
  target_compile_options(libvast_internal INTERFACE "-ftime-trace")
endif ()

# -- assertions ----------------------------------------------------------------

if (CMAKE_BUILD_TYPE STREQUAL Release)
  set(_VAST_ENABLE_ASSERTIONS_DEFAULT OFF)
else ()
  set(_VAST_ENABLE_ASSERTIONS_DEFAULT ON)
endif ()

option(VAST_ENABLE_ASSERTIONS "Enable assertions"
       "${_VAST_ENABLE_ASSERTIONS_DEFAULT}")
add_feature_info("VAST_ENABLE_ASSERTIONS" VAST_ENABLE_ASSERTIONS
                 "enable assertions.")

unset(_VAST_ENABLE_ASSERTIONS_DEFAULT)

# -- sanitizers ----------------------------------------------------------------

# Address Sanitizer
if ("${CMAKE_BUILD_TYPE}" IN_LIST "Debug;CI")
  set(_VAST_ENABLE_ASAN_DEFAULT ON)
else ()
  set(_VAST_ENABLE_ASAN_DEFAULT OFF)
endif ()

option(VAST_ENABLE_ASAN
       "Insert pointer and reference checks into the generated binaries"
       _VAST_ENABLE_ASAN_DEFAULT)
add_feature_info(
  "VAST_ENABLE_ASAN" VAST_ENABLE_ASAN
  "inserts pointer and reference checks into the generated binaries.")

unset(_VAST_ENABLE_ASAN_DEFAULT)

if (VAST_ENABLE_ASAN)
  target_compile_options(libvast_internal INTERFACE -fsanitize=address
                                                    -fno-omit-frame-pointer)
  target_link_libraries(libvast_internal INTERFACE -fsanitize=address)
endif ()

# Undefined Behavior Sanitizer
option(VAST_ENABLE_UBSAN
       "Add run-time checks for undefined behavior into the generated binaries"
       OFF)
add_feature_info(
  "VAST_ENABLE_UBSAN" VAST_ENABLE_UBSAN
  "adds run-time checks for undefined behavior into the generated binaries.")
if (VAST_ENABLE_UBSAN)
  target_compile_options(libvast_internal INTERFACE -fsanitize=undefined)
  target_link_libraries(libvast_internal INTERFACE -fsanitize=undefined)
endif ()

# -- static build support ------------------------------------------------------

cmake_dependent_option(
  VAST_ENABLE_STATIC_EXECUTABLE "Link VAST statically."
  "$ENV{VAST_ENABLE_STATIC_EXECUTABLE}" "NOT BUILD_SHARED_LIBS" OFF)
add_feature_info("VAST_ENABLE_STATIC_EXECUTABLE" VAST_ENABLE_STATIC_EXECUTABLE
                 "link VAST statically.")
if (VAST_ENABLE_STATIC_EXECUTABLE)
  target_link_libraries(libvast_internal INTERFACE -static-libgcc
                                                   -static-libstdc++ -static)
endif ()

if (VAST_ENABLE_STATIC_EXECUTABLE AND BUILD_SHARED_LIBS)
  message(FATAL_ERROR "Cannot create static binary with dynamic libraries")
endif ()

if (VAST_ENABLE_RELOCATABLE_INSTALLATIONS)
  # Note that we cannot use the target property INSTALL_RPATH on
  # libvast_internal, because it is an interface library. We should probably fix
  # this by making the config header part of libvast_internal so it is not just
  # an interface library.
  if (NOT VAST_ENABLE_STATIC_EXECUTABLE)
    if (APPLE)
      list(PREPEND CMAKE_INSTALL_RPATH
           "@executable_path/../${CMAKE_INSTALL_LIBDIR}")
    else ()
      list(PREPEND CMAKE_INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}")
    endif ()
  endif ()

  # For relocatable builds we need to find the relative difference between the
  # install prefix and the library location, or the binary location for static
  # VAST binaries.
  if (VAST_ENABLE_STATIC_EXECUTABLE)
    set(_libdir "${CMAKE_INSTALL_FULL_BINDIR}")
  else ()
    set(_libdir "${CMAKE_INSTALL_FULL_LIBDIR}")
  endif ()
  file(RELATIVE_PATH VAST_LIBDIR_TO_PREFIX "${_libdir}"
       "${CMAKE_INSTALL_PREFIX}")
  unset(_libdir)
endif ()

# -- jemalloc ------------------------------------------------------------------

option(VAST_ENABLE_JEMALLOC "Use jemalloc instead of libc malloc"
       "${VAST_ENABLE_STATIC_EXECUTABLE}")
add_feature_info("VAST_ENABLE_JEMALLOC" VAST_ENABLE_JEMALLOC
                 "use jemalloc instead of libc malloc.")
if (VAST_ENABLE_JEMALLOC)
  find_package(jemalloc REQUIRED)
  provide_find_module(jemalloc)
  string(APPEND VAST_FIND_DEPENDENCY_LIST "\nfind_package(jemalloc REQUIRED)")
  target_link_libraries(libvast_internal INTERFACE jemalloc::jemalloc_)
  dependency_summary("jemalloc" jemalloc::jemalloc_ "Dependencies")
endif ()

# -- developer mode ------------------------------------------------------------

option(VAST_ENABLE_CLANG_TIDY "Run clang-tidy during build" OFF)
add_feature_info("VAST_ENABLE_CLANG_TIDY" VAST_ENABLE_CLANG_TIDY
                 "run clang-tidy during build.")
if (VAST_ENABLE_CLANG_TIDY)
  macro (VASTTargetEnableTooling _target)
    set(_clang_tidy_args
        "clang-tidy;--config-file=${PROJECT_SOURCE_DIR}/.clang-tidy")
    set_target_properties(
      ${_target} PROPERTIES CXX_CLANG_TIDY "${_clang_tidy_args}"
                            C_CLANG_TIDY "${_clang_tidy_args}")
  endmacro ()
else ()
  macro (VASTTargetEnableTooling _target)

  endmacro ()
endif ()

# Build VAST in developer mode. This is enabled by default when not building
# VAST as a subproject. The developer mode contains CCache support and many
# other niceties.
option(VAST_ENABLE_DEVELOPER_MODE
       "Enables build settings for a nicer development environment"
       "${VAST_IS_NOT_SUBPROJECT}")
add_feature_info("VAST_ENABLE_DEVELOPER_MODE" VAST_ENABLE_DEVELOPER_MODE
                 "enables build settings for a nicer development environment.")

if (VAST_ENABLE_DEVELOPER_MODE)
  # Support tools like clang-tidy by creating a compilation database and
  # copying it to the project root.
  VASTExportCompileCommands(libvast_internal)

  # Force colored output for the Ninja generator
  if ("${CMAKE_GENERATOR}" MATCHES "Ninja")
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
      target_compile_options(libvast_internal
                             INTERFACE -fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
      target_compile_options(libvast_internal INTERFACE -fcolor-diagnostics)
    endif ()
  endif ()

  # Keep make output sane
  set(CMAKE_VERBOSE_MAKEFILE
      OFF
      CACHE STRING "Show all outputs including compiler lines." FORCE)

  # Enable CAF's compile-time type ID checks.
  target_compile_definitions(libvast_internal
                             INTERFACE CAF_ENABLE_TYPE_ID_CHECKS)
endif ()

if (VAST_ENABLE_DEVELOPER_MODE OR VAST_ENABLE_BUILDID)
  # Relocate debug paths to a common prefix for CCache users that work from
  # multiple worktrees.
  # The debug paths affect the build-id, we rewrite them to get a more
  # reproducible build.
  target_compile_options(
    libvast_internal
    INTERFACE "-fdebug-prefix-map=${CMAKE_CURRENT_SOURCE_DIR}=.")
endif ()

# -- additional warnings -------------------------------------------------------

option(VAST_ENABLE_MORE_WARNINGS "Enable addditional warnings" OFF)
add_feature_info("VAST_ENABLE_MORE_WARNINGS" VAST_ENABLE_MORE_WARNINGS
                 "enable additional warnings.")
if (VAST_ENABLE_MORE_WARNINGS)
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(
      libvast_internal
      INTERFACE -Weverything
                -Wno-c++98-compat
                -Wno-padded
                -Wno-documentation-unknown-command
                -Wno-exit-time-destructors
                -Wno-global-constructors
                -Wno-missing-prototypes
                -Wno-c++98-compat-pedantic
                -Wno-unused-member-function
                -Wno-unused-const-variable
                -Wno-switch-enum
                -Wno-abstract-vbase-init
                -Wno-missing-noreturn
                -Wno-covered-switch-default)
  elseif (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    target_compile_options(
      libvast_internal
      INTERFACE -Waddress
                -Wall
                -Warray-bounds
                -Wattributes
                -Wbuiltin-macro-redefined
                -Wcast-align
                -Wcast-qual
                -Wchar-subscripts
                -Wclobbered
                -Wcomment
                -Wconversion
                -Wconversion-null
                -Wcoverage-mismatch
                -Wcpp
                -Wdelete-non-virtual-dtor
                -Wdeprecated
                -Wdeprecated-declarations
                -Wdiv-by-zero
                -Wdouble-promotion
                -Wempty-body
                -Wendif-labels
                -Wenum-compare
                -Wextra
                -Wfloat-equal
                -Wformat
                -Wfree-nonheap-object
                -Wignored-qualifiers
                -Winit-self
                -Winline
                -Wint-to-pointer-cast
                -Winvalid-memory-model
                -Winvalid-offsetof
                -Wlogical-op
                -Wmain
                -Wmaybe-uninitialized
                -Wmissing-braces
                -Wmultichar
                -Wnarrowing
                -Wnoexcept
                -Wnon-template-friend
                -Wnon-virtual-dtor
                -Wnonnull
                -Woverflow
                -Woverlength-strings
                -Wparentheses
                -Wpmf-conversions
                -Wpointer-arith
                -Wreorder
                -Wreturn-type
                -Wsequence-point
                -Wsign-compare
                -Wswitch
                -Wtype-limits
                -Wundef
                -Wuninitialized
                -Wunused
                -Wvla
                -Wwrite-strings
                -Wno-redundant-move)
  endif ()
endif ()

# -- compiler setup ------------------------------------------------------------

set(CLANG_MINIMUM_VERSION 11.0)
set(APPLE_CLANG_MINIMUM_VERSION 12.0.5)
set(GCC_MINIMUM_VERSION 10.0)

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "GNU")
  check_compiler_version(${GCC_MINIMUM_VERSION})
elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    check_compiler_version(${APPLE_CLANG_MINIMUM_VERSION})
  else ()
    check_compiler_version(${CLANG_MINIMUM_VERSION})
  endif ()
else ()
  message(WARNING "Unsupported compiler: ${CMAKE_CXX_COMPILER_ID}")
endif ()

# -- schemas -------------------------------------------------------------------

option(VAST_ENABLE_BUNDLED_SCHEMAS "Install bundled schemas with VAST" ON)
add_feature_info("VAST_ENABLE_BUNDLED_SCHEMAS" VAST_ENABLE_BUNDLED_SCHEMAS
                 "install bundled schemas with VAST.")
if (VAST_ENABLE_RELOCATABLE_INSTALLATIONS)
  add_custom_target(
    vast-schema
    COMMAND
      ${CMAKE_COMMAND} -E copy_directory "${CMAKE_CURRENT_SOURCE_DIR}/schema"
      "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_DATADIR}/vast/schema/"
    COMMENT "Copying schema directory")
endif ()

if (VAST_ENABLE_BUNDLED_SCHEMAS)
  install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/schema"
          DESTINATION "${CMAKE_INSTALL_DATADIR}/vast")
endif ()

# -- documentation headers -----------------------------------------------------

# TODO: Move this to libvast/CMakeLists.txt, somehow.

file(
  GLOB doc_files CONFIGURE_DEPENDS
  RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
  "doc/cli/*.md")
list(SORT doc_files)
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

# -- add subdirectories --------------------------------------------------------

add_subdirectory(libvast)
add_subdirectory(libvast_test)
add_subdirectory(tools)
add_subdirectory(doc)
add_subdirectory(vast)

# -- cmake export/config installations -----------------------------------------

export(
  EXPORT VASTTargets
  FILE VASTTargets.cmake
  NAMESPACE vast::)

install(
  EXPORT VASTTargets
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vast"
  NAMESPACE vast::)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/VASTConfigVersion.cmake"
  VERSION "${VAST_VERSION_MAJOR}.${VAST_VERSION_MINOR}.${VAST_VERSION_PATCH}"
  COMPATIBILITY ExactVersion)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/VASTConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/VASTConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vast")

install(
  FILES "${CMAKE_CURRENT_SOURCE_DIR}/cmake/VASTRegisterPlugin.cmake"
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/VASTMacDependencyPaths.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/VASTConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/VASTConfigVersion.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vast")

# -- scripts -------------------------------------------------------------------

# TODO: Make other scripts installable as well.

option(VAST_ENABLE_CORELIGHT_CAT "Install corelight-cat with VAST" OFF)
add_feature_info("VAST_ENABLE_CORELIGHT_CAT" VAST_ENABLE_CORELIGHT_CAT
                 "install corelight-cat with VAST.")
if (VAST_ENABLE_CORELIGHT_CAT)
  install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/scripts/corelight-cat"
          DESTINATION ${CMAKE_INSTALL_BINDIR})
endif ()

# -- docdir --------------------------------------------------------------------

# Install LICENSE and README. This can intentionally not be disabled.
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/CHANGELOG.md"
        "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE"
        "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE.3rdparty"
        "${CMAKE_CURRENT_SOURCE_DIR}/README.md"
  DESTINATION "${CMAKE_INSTALL_DOCDIR}")

# -- feature summary -----------------------------------------------------------

# Append the feature summary to summary.log.
feature_summary(
  WHAT ALL
  FILENAME ${CMAKE_CURRENT_BINARY_DIR}/summary.log
  APPEND)

# Print the feature and build summary if we're not a subproject.
if (NOT VAST_IS_SUBPROJECT)
  message("\n")
  feature_summary(WHAT ENABLED_FEATURES DISABLED_FEATURES
                  INCLUDE_QUIET_PACKAGES)
  message(STATUS "Build summary:\n")
  message(" * Version: ${VAST_VERSION_TAG}")
  message(" * Build Tree Hash: ${VAST_BUILD_TREE_HASH}")
  message("")
  if (CMAKE_CONFIGURATION_TYPES)
    message(" * Configuration Types: ${CMAKE_CONFIGURATION_TYPES}")
  else ()
    message(" * Build Type: ${CMAKE_BUILD_TYPE}")
  endif ()
  message(" * Source Directory: ${CMAKE_SOURCE_DIR}")
  message(" * Binary Directory: ${CMAKE_BINARY_DIR}\n")
  message(" * System Name: ${CMAKE_SYSTEM_NAME}")
  message(" * CPU: ${CMAKE_SYSTEM_PROCESSOR}")
  foreach (lang IN ITEMS C CXX)
    set(_lang_compiler "${CMAKE_${lang}_COMPILER}")
    set(_lang_compiler_info
        "${CMAKE_${lang}_COMPILER_ID} ${CMAKE_${lang}_COMPILER_VERSION}")
    set(_lang_flags
        "${CMAKE_${lang}_FLAGS} ${CMAKE_${lang}_FLAGS_${CMAKE_BUILD_TYPE}}
      ${CMAKE_CPP_FLAGS} ${CMAKE_CPP_FLAGS_${CMAKE_BUILD_TYPE}}")
    string(STRIP "${_lang_flags}" _lang_flags)
    if (_lang_flags)
      message(
        " * ${lang} Compiler: ${_lang_compiler} (${_lang_compiler_info} with ${_lang_flags})"
      )
    else ()
      message(" * ${lang} Compiler: ${_lang_compiler} (${_lang_compiler_info})")
    endif ()
  endforeach ()
  message(" * Linker: ${CMAKE_LINKER}")
  message(" * Archiver: ${CMAKE_AR}")
  get_property(VAST_DEPENDENCY_SUMMARY_CATEGORIES GLOBAL
               PROPERTY "VAST_DEPENDENCY_SUMMARY_CATEGORIES_PROPERTY")
  foreach (category IN LISTS VAST_DEPENDENCY_SUMMARY_CATEGORIES)
    get_property(VAST_DEPENDENCY_SUMMARY GLOBAL
                 PROPERTY "VAST_DEPENDENCY_SUMMARY_${category}_PROPERTY")
    if (VAST_DEPENDENCY_SUMMARY)
      message("")
      message("${category}:")
      foreach (summary IN LISTS VAST_DEPENDENCY_SUMMARY)
        message("${summary}")
      endforeach ()
    endif ()
  endforeach ()
  message("")
endif ()
