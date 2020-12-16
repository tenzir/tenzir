cmake_minimum_required(VERSION 3.14...3.19 FATAL_ERROR)

# -- project setup -------------------------------------------------------------

# Semicolon-separated list of directories specifying a search path for CMake.
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# Helper utility for retrieving the project version from 'git-describe', that is
# bundled with VAST.
include(VASTGitVersion)

# Sets the fallback version of VAST. This version number is only used if the
# version cannot be retrieved using 'git-describe'
if (NOT DEFINED ENV{VAST_VERSION_TAG})
  option(VAST_VERSION_TAG "Sets the fallback version of VAST" "2020.10.29-0-")
else ()
  set(VAST_VERSION_TAG "${ENV{VAST_VERSION_TAG}}")
endif ()

# Create the VAST project.
project(
  VAST
  VERSION
    ${VAST_VERSION_MAJOR}.${VAST_VERSION_MINOR}.${VAST_VERSION_PATCH}.${VAST_VERSION_TWEAK}
  DESCRIPTION "Visibility Across Space and Time"
  LANGUAGES C CXX)

# -- sanity checks -------------------------------------------------------------

# Prohibit in-source builds.
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
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

# -- silence policy-related warnings -------------------------------------------

# TODO: When updating to CAF 0.18, this should no longer be needed.
if (POLICY CMP0048)
  unset(PROJECT_VERSION)
  unset(PROJECT_VERSION_MAJOR)
  unset(PROJECT_VERSION_MINOR)
  unset(PROJECT_VERSION_PATCH)
  unset(PROJECT_VERSION_TWEAK)
endif ()

if (POLICY CMP0042)
  if (APPLE AND NOT DEFINED CMAKE_MACOSX_RPATH)
    set(CMAKE_SKIP_BUILD_RPATH FALSE)
    set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
    if (NOT VAST_RELOCATABLE_INSTALL)
      set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
    endif ()
  endif ()
endif ()

# -- internal target -----------------------------------------------------------

# Create the internal target that is used for option/property propagation.
add_library(libvast_internal INTERFACE)
set_target_properties(libvast_internal PROPERTIES EXPORT_NAME internal)
add_library(vast::internal ALIAS libvast_internal)
install(TARGETS libvast_internal EXPORT VASTTargets)

# Require standard C++17
target_compile_features(libvast_internal INTERFACE cxx_std_17)

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

# TODO add option to disable SSE

target_compile_options(
  libvast_internal
  INTERFACE $<$<CONFIG:Release>:-msse3
            -mssse3
            -msse4.1
            -msse4.2
            -fno-omit-frame-pointer>
            $<$<CONFIG:RelWithDebInfo>:-fno-omit-frame-pointer>
            $<$<CONFIG:Debug>:-O0>
            $<$<CONFIG:CI>:-O2
            -g1>)

# -- warnings ------------------------------------------------------------------

target_compile_options(
  libvast_internal
  INTERFACE -Wall
            -Wextra
            -pedantic
            -Werror=switch
            $<$<CONFIG:CI>:-Werror
            -Wno-error=deprecated
            -Wno-error=deprecated-declarations>)

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
if (VAST_ENABLE_SDT)
  target_compile_definitions(libvast_internal INTERFACE VAST_ENABLE_SDT=1)
endif ()

# -- exceptions ----------------------------------------------------------------

option(VAST_ENABLE_EXCEPTIONS "Enable C++ exceptions" ON)
add_feature_info("VAST_ENABLE_EXCEPTIONS" VAST_ENABLE_EXCEPTIONS
                 "use C++ exceptions.")
if (NOT VAST_ENABLE_EXCEPTIONS)
  target_compile_definitions(libvast_internal INTERFACE VAST_NO_EXCEPTIONS=1)
  target_compile_options(libvast_internal INTERFACE "-fno-exceptions")
endif ()

# -- build profiling -----------------------------------------------------------

option(VAST_ENABLE_TIME_REPORT
       "Print information where time was spent during compilation" OFF)
add_feature_info("VAST_ENABLE_TIME_REPORT" VAST_ENABLE_TIME_REPORT
                 "print information where time was spent during compilation.")
if (SHOW_TIME_REPORT)
  target_compile_options(libvast_internal INTERFACE "-ftine-report")
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

if (VAST_ENABLE_ASSERTIONS)
  find_package(Backtrace REQUIRED)
  target_include_directories(libvast_internal
                             PUBLIC ${Backtrace_INCLUDE_DIRECTORIES})
  target_link_libraries(libvast_internal PUBLIC ${Backtrace_LIBRARIES})
endif ()

unset(_VAST_ENABLE_ASSERTIONS_DEFAULT)

# -- sanitizers ----------------------------------------------------------------

# Address Sanitizer
option(VAST_ENABLE_ASAN
       "Insert pointer and reference checks into the generated binaries" OFF)
add_feature_info(
  "VAST_ENABLE_ASAN" VAST_ENABLE_ASAN
  "inserts pointer and reference checks into the generated binaries.")
if (VAST_ENABLE_ASAN)
  target_compile_options(libvast_internal INTERFACE -fsanitize=address
                                                    -fno-omit-frame-pointer)
  target_link_libraries(libvast_internal INTERFACE -fsanitize=address
                                                   -fno-omit-frame-pointer)
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

# -- relocatability and static build support -----------------------------------

# Setting this option removes configuration dependent absolute paths from the
# VAST installation. Concretely, it enables the dynamic binary and libraries to
# use relative paths for loading their dependencies.
option(VAST_ENABLE_RELOCATABLE_INSTALLATIONS "Enable relocatable installations"
       ON)
add_feature_info(
  "VAST_ENABLE_RELOCATABLE_INSTALLATIONS" VAST_ENABLE_RELOCATABLE_INSTALLATIONS
  "enable relocatable installations.")

option(VAST_ENABLE_STATIC_EXECUTABLE "Link VAST statically."
       $ENV{VAST_ENABLE_STATIC_EXECUTABLE})
add_feature_info("VAST_ENABLE_STATIC_EXECUTABLE" BUILD_SHARED_LIBS
                 "link VAST statically.")
if (VAST_ENABLE_STATIC_EXECUTABLE)
  target_link_libraries(libvast_internal INTERFACE -static-libgcc
                                                   -static-libstdc++ -static)
endif ()

cmake_dependent_option(
  BUILD_SHARED_LIBS "Build shared instead of static libraries" ON
  "NOT VAST_STATIC_EXECUTABLE" OFF)
add_feature_info("BUILD_SHARED_LIBS" BUILD_SHARED_LIBS
                 "build shared instead of static library.")
if (BUILD_SHARED_LIBS AND "${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
  target_link_libraries(libvast_internal PUBLIC dl)
endif ()

if (VAST_STATIC_EXECUTABLE AND BUILD_SHARED_LIBS)
  message(FATAL_ERROR "Cannot create static binary with dynamic libraries")
endif ()

if (VAST_RELOCATABLE_INSTALL AND NOT VAST_STATIC_EXECUTABLE)
  if (APPLE)
    set_target_properties(
      libvast_internal
      PROPERTIES
        INSTALL_RPATH
        "@executable_path/../${CMAKE_INSTALL_LIBDIR};${CMAKE_INSTALL_RPATH}")
  else ()
    # Remove escaping of ${ORIGIN} when requiring CMake>=3.16. See CMP0095.
    set_target_properties(
      libvast_internal
      PROPERTIES
        INSTALL_RPATH
        "\\\${ORIGIN}/../${CMAKE_INSTALL_LIBDIR};${CMAKE_INSTALL_RPATH}")
  endif ()
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
  target_compile_definitions(libvast_internal INTERFACE VAST_HAVE_JEMALLOC=1)
  target_link_libraries(libvast_internal INTERFACE jemalloc::jemalloc_)
endif ()

# -- developer mode ------------------------------------------------------------

# Build VAST in developer mode. This is enabled by default when not building
# VAST as a subproject. The developer mode contains CCache support and many
# other niceties.
option(VAST_ENABLE_DEVELOPER_MODE
       "Enables build settings for a nicer development environment"
       "${VAST_IS_NOT_SUBPROJECT}")
add_feature_info("VAST_ENABLE_DEVELOPER_MODE" VAST_ENABLE_DEVELOPER_MODE
                 "enables build settings for a nicer development environment.")

if (VAST_ENABLE_DEVELOPER_MODE)
  # Support tools like clang-tidy by creating a compilation database.
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

  # Link the compilation database to the project root by default.
  add_custom_target(
    link_compilation_database ALL
    COMMAND
      ${CMAKE_COMMAND} -E create_symlink
      "${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json" compile_commands.json
    BYPRODUCTS compile_commands.json
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Linking compilation database")

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

  # Relocate debug paths to a common prefix for CCache users that work from
  # multiple worktrees.
  target_compile_options(
    libvast_internal
    INTERFACE "-fdebug-prefix-map=${CMAKE_CURRENT_SOURCE_DIR}=.")
endif ()

# -- additional warnings -------------------------------------------------------

option(VAST_ENABLE_MORE_WARNINGS "Enable addditional warnings"
       "${VAST_ENABLE_STATIC_EXECUTABLE}")
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
                -Wno-covered-switch-default
                -Wno-unknown-warning-option)
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
                -Wno-unknown-warning
                -Wno-redundant-move)
  endif ()
endif ()

# -- single output directory ---------------------------------------------------

# Use a single output directory for executable and library files.
# TODO These are deprecated; do we actually need this?
set(EXECUTABLE_OUTPUT_PATH
    ${CMAKE_CURRENT_BINARY_DIR}/bin
    CACHE PATH "Single directory for all executables")
set(LIBRARY_OUTPUT_PATH
    ${CMAKE_CURRENT_BINARY_DIR}/lib
    CACHE PATH "Single directory for all libraries")

# -- compiler setup ------------------------------------------------------------

set(CLANG_MINIMUM_VERSION 8.0)
set(APPLE_CLANG_MINIMUM_VERSION 9.1)
set(GCC_MINIMUM_VERSION 8.0)

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

# -- documentation headers -----------------------------------------------------

# TODO: Move this to libvast/CMakeLists.txt, somehow.

file(
  GLOB doc_files CONFIGURE_DEPENDS
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
  DESTINATION "${CMAKE_INSTALL_FULL_LIBDIR}/cmake/vast"
  NAMESPACE vast::)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/VASTConfigVersion.cmake"
  VERSION
    "${VAST_VERSION_MAJOR}.${VAST_VERSION_MINOR}.${VAST_VERSION_PATCH}.${VAST_VERSION_TWEAK}"
  COMPATIBILITY ExactVersion)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/VASTConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/VASTConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_FULL_LIBDIR}/cmake/vast")

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/VASTConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/VASTConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_FULL_LIBDIR}/cmake/vast")

# -- scripts -------------------------------------------------------------------

# TODO: Make other scripts installable as well.

option(VAST_ENABLE_CORELIGHT_CAT "Install corelight-cat with VAST" OFF)
add_feature_info("VAST_ENABLE_CORELIGHT_CAT" VAST_ENABLE_CORELIGHT_CAT
                 "install corelight-cat with VAST.")
if (VAST_ENABLE_CORELIGHT_CAT)
  install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/scripts/corelight-cat"
          DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})
endif ()

# -- schemas -------------------------------------------------------------------

add_custom_target(
  vast-schema
  COMMAND
    ${CMAKE_COMMAND} -E copy_directory "${CMAKE_CURRENT_SOURCE_DIR}/schema"
    "${CMAKE_BINARY_DIR}/share/vast/schema/"
  COMMENT "Copying schema directory")

option(VAST_ENABLE_BUNDLED_SCHEMAS "Install bundled schemas with VAST" ON)
add_feature_info("VAST_ENABLE_BUNDLED_SCHEMAS" VAST_ENABLE_BUNDLED_SCHEMAS
                 "install bundled schemas with VAST.")
if (VAST_ENABLE_BUNDLED_SCHEMAS)
  install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/schema"
          DESTINATION "${CMAKE_INSTALL_FULL_DATADIR}/vast")
endif ()

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/vast.yaml.example"
        DESTINATION "${CMAKE_INSTALL_FULL_DATADIR}/vast/")

# -- docdir --------------------------------------------------------------------

# Install LICENSE and README. This can intentionally not be disabled.
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE"
              "${CMAKE_CURRENT_SOURCE_DIR}/README.md"
        DESTINATION "${CMAKE_INSTALL_FULL_DOCDIR}")

# -- feature summary -----------------------------------------------------------

# Append the feature summary to summary.log.
feature_summary(
  WHAT ALL
  FILENAME ${CMAKE_CURRENT_BINARY_DIR}/summary.log
  APPEND)

# Print the feature summary if we're not a subproject.
if (NOT VAST_IS_SUBPROJECT)
  feature_summary(WHAT ENABLED_FEATURES DISABLED_FEATURES
                  INCLUDE_QUIET_PACKAGES)
endif ()
