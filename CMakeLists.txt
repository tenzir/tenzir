cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

# -- project setup -------------------------------------------------------------

# Semicolon-separated list of directories specifying a search path for CMake.
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
if (DEFINED TENZIR_VERSION_BUILD_METADATA)
  set(TENZIR_VERSION_BUILD_METADATA_BACKUP "${TENZIR_VERSION_BUILD_METADATA}")
endif ()
include(TenzirVersion)

# Create the Tenzir project.
project(
  Tenzir
  VERSION "${TENZIR_VERSION_MMP}"
  DESCRIPTION "Visibility Across Space and Time"
  HOMEPAGE_URL "https://tenzir.com"
  LANGUAGES C CXX)

set(TENZIR_EDITION_NAME
    "Tenzir"
    CACHE STRING "Set the edition name")

add_compile_options("-ffile-prefix-map=${CMAKE_CURRENT_SOURCE_DIR}/=")

# -- sanity checks -------------------------------------------------------------

# Prohibit in-source builds.
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif ()

# Check whether remnants of an earlier in-source build exist.
if (EXISTS "${CMAKE_SOURCE_DIR}/CMakeCache.txt"
    OR EXISTS "${CMAKE_SOURCE_DIR}/CMakeFiles")
  message(
    FATAL_ERROR
      "Detected an earlier in-source build; please remove CMakeCache.txt and CMakeFiles/ from your source-directory"
  )
endif ()

# Ensure that CMAKE_INSTALL_PREFIX is not a relative path.
if (NOT IS_ABSOLUTE "${CMAKE_INSTALL_PREFIX}")
  message(
    FATAL_ERROR
      "CMAKE_INSTALL_PREFIX must be an absolute path: ${CMAKE_INSTALL_PREFIX}")
endif ()

# Override `find_package` for subprojects to be a no-op when trying to find Tenzir
# itself. This simplifies the plugin CMake by not requiring plugin developers to
# check whether their plugin is built alongside Tenzir.
if ("${CMAKE_PROJECT_NAME}" STREQUAL "${PROJECT_NAME}")
  macro (find_package)
    if (NOT "${ARGV0}" STREQUAL "${CMAKE_PROJECT_NAME}")
      _find_package(${ARGV})
    endif ()
  endmacro ()
endif ()

# -- Homebrew configuration mapping --------------------------------------------

# Homebrew packages are typically built with Release configuration only, so
# their CMake configs only export IMPORTED_LOCATION_RELEASE. When building with
# other configurations (Debug, RelWithDebInfo), CMake can't find the imported
# location and falls back to using the target name as a linker flag (e.g.,
# -lgflags_shared), which fails. This mapping tells CMake to use the Release
# configuration of imported targets when building other configurations.
#
# We also include an empty string "" as a fallback for packages that use
# config-mode find_package but only set IMPORTED_LOCATION (no config suffix).
if (APPLE)
  set(CMAKE_MAP_IMPORTED_CONFIG_DEBUG Release "")
  set(CMAKE_MAP_IMPORTED_CONFIG_RELWITHDEBINFO Release "")
  set(CMAKE_MAP_IMPORTED_CONFIG_MINSIZEREL Release "")
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
include(CTest)
include(CheckCXXSourceCompiles)
include(CheckLibraryExists)
include(GNUInstallDirs)
include(TenzirCheckCompilerVersion)
include(TenzirMacDependencyPaths)
include(TenzirProvideFindModule)
include(TenzirRegisterPlugin)
include(TenzirUtilities)

# -- extra installation dirs ---------------------------------------------------

set(TENZIR_INSTALL_ARCHIVEDIR
    "${CMAKE_INSTALL_LIBDIR}"
    CACHE PATH "The Installation path of static archives")

set(TENZIR_INSTALL_CMAKEDIR
    "${CMAKE_INSTALL_LIBDIR}/cmake"
    CACHE PATH "The Installation path of the cmake files")

# -- project configuration -----------------------------------------------------

# Determine whether we're building Tenzir a subproject. This is used to determine
# good default values for many options. Tenzir should not modify global CMake if
# built as a subproject, unless explicitly requested to do so with options.
get_directory_property(_TENZIR_PARENT_DIRECTORY PARENT_DIRECTORY)
if (_TENZIR_PARENT_DIRECTORY)
  set(TENZIR_IS_SUBPROJECT ON)
  set(TENZIR_IS_NOT_SUBPROJECT OFF)
else ()
  set(TENZIR_IS_SUBPROJECT OFF)
  set(TENZIR_IS_NOT_SUBPROJECT ON)
endif ()
unset(_TENZIR_PARENT_DIRECTORY)

# -- testing -------------------------------------------------------------------

cmake_dependent_option(
  TENZIR_ENABLE_UNIT_TESTS "Build unit tests for libtenzir" ON
  "NOT CMAKE_CROSS_COMPILING" OFF)
set(TENZIR_UNIT_TEST_TIMEOUT
    "60"
    CACHE STRING "The per-test timeout in unit tests" FORCE)

# -- library flavor ------------------------------------------------------------

option(BUILD_SHARED_LIBS "Build shared instead of static libraries" ON)

# -- relocatable installations -------------------------------------------------

# Setting this option removes configuration dependent absolute paths from the
# Tenzir installation. Concretely, it enables the dynamic binary and libraries to
# use relative paths for loading their dependencies. On macOS, always enable
# relocatable installations as it's very uncommon not to do so.
cmake_dependent_option(TENZIR_ENABLE_RELOCATABLE_INSTALLATIONS
                       "Enable relocatable installations" ON "NOT APPLE" ON)

if (TENZIR_ENABLE_RELOCATABLE_INSTALLATIONS)
  # For relocatable builds we need the configured install directories to be
  # relative so we can interpret them relative to the object path of the running
  # binary instead of a fixed install prefix.
  TenzirNormalizeInstallDirs()

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
if (NOT TENZIR_ENABLE_RELOCATABLE_INSTALLATIONS)
  install(
    CODE "\
    cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)
    get_filename_component(build_time_prefix
      \"\$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}\" ABSOLUTE)
    get_filename_component(install_time_prefix
      \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}\" ABSOLUTE)
    if (NOT \"\${build_time_prefix}\" STREQUAL \"\${install_time_prefix}\")
      message(FATAL_ERROR
        \"For non-relocatable builds the configured build-time install prefix (\${build_time_prefix}) must match the configured install-time install prefix (\${install_time_prefix})\"
      )
    endif ()
    "
    COMPONENT Development)
endif ()

# -- internal target -----------------------------------------------------------

# Create the internal target that is used for option/property propagation.
add_library(libtenzir_internal INTERFACE)
set_target_properties(libtenzir_internal PROPERTIES EXPORT_NAME internal)
add_library(tenzir::internal ALIAS libtenzir_internal)
install(
  TARGETS libtenzir_internal
  EXPORT TenzirTargets
  COMPONENT Development)

# Require standard C++26.
target_compile_features(libtenzir_internal INTERFACE cxx_std_23)

# Enable coroutines for GCC < 11.
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION
                                             VERSION_LESS 11)
  target_compile_options(libtenzir_internal INTERFACE -fcoroutines)
endif ()

# Increase maximum number of template instantiations.
target_compile_options(libtenzir_internal
                       INTERFACE -ftemplate-backtrace-limit=0)

# -- FreeBSD support -----------------------------------------------------------

if (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
  # Works around issues with libstdc++ and C++11. For details, see: -
  # https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=194929 -
  # https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=182657
  target_compile_definitions(
    libtenzir_internal INTERFACE _GLIBCXX_USE_C99 _GLIBCXX_USE_C99_MATH
                                 _GLIBCXX_USE_C99_MATH_TR1)
endif ()

# -- optimizations -------------------------------------------------------------

option(CMAKE_INTERPROCEDURAL_OPTIMIZATION_Release
       "Enable link time optimizations for the Release build" ON)

option(TENZIR_ENABLE_AUTO_VECTORIZATION "Enable SSE instructions" ON)
if (TENZIR_ENABLE_AUTO_VECTORIZATION)
  find_package(SSE QUIET)
  option(TENZIR_ENABLE_SSE_INSTRUCTIONS "Enable SSE instructions"
         "${SSE_FOUND}")
  option(TENZIR_ENABLE_SSE2_INSTRUCTIONS "Enable SSE2 instructions"
         "${SSE2_FOUND}")
  option(TENZIR_ENABLE_SSE3_INSTRUCTIONS "Enable SSE3 instructions"
         "${SSE3_FOUND}")
  option(TENZIR_ENABLE_SSSE3_INSTRUCTIONS "Enable SSSE3 instructions"
         "${SSSE3_FOUND}")
  option(TENZIR_ENABLE_SSE4_1_INSTRUCTIONS "Enable SSE4.1 instructions"
         "${SSE4_1_FOUND}")
  option(TENZIR_ENABLE_SSE4_2_INSTRUCTIONS "Enable SSE4.2 instructions"
         "${SSE4_2_FOUND}")
  option(TENZIR_ENABLE_AVX_INSTRUCTIONS "Enable AVX instructions"
         "${AVX_FOUND}")
  option(TENZIR_ENABLE_AVX2_INSTRUCTIONS "Enable AVX2 instructions"
         "${AVX2_FOUND}")
endif ()

target_compile_options(
  libtenzir_internal
  INTERFACE $<$<CONFIG:Release>:
            -fno-omit-frame-pointer>
            $<$<CONFIG:RelWithDebInfo>:-fno-omit-frame-pointer>
            $<$<CONFIG:Debug>:-O0>
            $<$<CONFIG:CI>:-O2
            -g1>
            $<$<BOOL:${TENZIR_ENABLE_SSE_INSTRUCTIONS}>:-msse>
            $<$<BOOL:${TENZIR_ENABLE_SSE2_INSTRUCTIONS}>:-msse2>
            $<$<BOOL:${TENZIR_ENABLE_SSE3_INSTRUCTIONS}>:-msse3>
            $<$<BOOL:${TENZIR_ENABLE_SSSE3_INSTRUCTIONS}>:-mssse3>
            $<$<BOOL:${TENZIR_ENABLE_SSE4_1_INSTRUCTIONS}>:-msse4.1>
            $<$<BOOL:${TENZIR_ENABLE_SSE4_2_INSTRUCTIONS}>:-msse4.2>
            $<$<BOOL:${TENZIR_ENABLE_AVX_INSTRUCTIONS}>:-mavx>
            $<$<BOOL:${TENZIR_ENABLE_AVX2_INSTRUCTIONS}>:-mavx2>)

# -- warnings ------------------------------------------------------------------

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(libtenzir_internal
                         INTERFACE -Wno-unknown-warning-option)
  target_compile_options(libtenzir_internal
                         INTERFACE -Wno-unknown-warning-option)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  target_compile_options(libtenzir_internal INTERFACE -Wno-unknown-warning)
endif ()

set(gcc12plus
    "$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_GREATER_EQUAL:$<CXX_COMPILER_VERSION>,12>>"
)
target_compile_options(
  libtenzir_internal
  INTERFACE -Wall
            -Wextra
            -pedantic
            -Werror=switch
            -Werror=odr
            -Wundef
            $<$<CONFIG:CI>:-Werror
            # Spurious warnings when ASAN is combined with optimizations.
            $<$<AND:$<CONFIG:CI>,${gcc12plus}>:-Wno-maybe-uninitialized>
            $<$<AND:$<CONFIG:CI>,${gcc12plus}>:-Wno-restrict>
            # Never treat deprecation warnings as errors.
            -Wno-error=deprecated
            -Wno-error=deprecated-declarations>
            $<$<CXX_COMPILER_ID:GNU>:-Wno-interference-size>
            $<$<CXX_COMPILER_ID:GNU>:-Wno-error=bool-compare>)

# -- build id ------------------------------------------------------------------

cmake_dependent_option(
  TENZIR_ENABLE_BUILDID "Include a unique identifier in the elf notes section"
  ON "CMAKE_EXECUTABLE_FORMAT STREQUAL ELF" OFF)
if (TENZIR_ENABLE_BUILDID)
  target_link_options(libtenzir_internal INTERFACE "-Wl,--build-id")
endif ()

# -- USDT tracepoints ----------------------------------------------------------

option(TENZIR_ENABLE_SDT "Generate USDT tracepoint instrumentation" ON)

# -- build profiling -----------------------------------------------------------

option(TENZIR_ENABLE_TIME_REPORT
       "Print information where time was spent during compilation" OFF)
if (TENZIR_ENABLE_TIME_REPORT)
  target_compile_options(libtenzir_internal INTERFACE "-ftime-report")
endif ()

option(TENZIR_ENABLE_TIME_TRACE
       "Generate tracing JSON for compilation time profiling" OFF)
if (TENZIR_ENABLE_TIME_TRACE)
  include(CheckCXXCompilerFlag)
  check_cxx_compiler_flag("-ftime-trace" _time_trace_supported)
  if (NOT _time_trace_supported)
    message(
      FATAL_ERROR
        "-ftime-trace option not supported by compiler ${CMAKE_CXX_COMPILER}")
  endif ()
  unset(_time_trace_supported)
  target_compile_options(libtenzir_internal INTERFACE "-ftime-trace")
endif ()

# -- assertions ----------------------------------------------------------------

if (CMAKE_BUILD_TYPE STREQUAL Release)
  set(_TENZIR_ENABLE_ASSERTIONS_DEFAULT OFF)
else ()
  set(_TENZIR_ENABLE_ASSERTIONS_DEFAULT ON)
endif ()

option(TENZIR_ENABLE_ASSERTIONS "Enable assertions"
       "${_TENZIR_ENABLE_ASSERTIONS_DEFAULT}")

unset(_TENZIR_ENABLE_ASSERTIONS_DEFAULT)

option(TENZIR_ENABLE_ASSERTIONS_CHEAP "Enable cheap assertions" ON)

# -- static build support ------------------------------------------------------

cmake_dependent_option(
  TENZIR_ENABLE_STATIC_EXECUTABLE "Link Tenzir statically."
  "$ENV{TENZIR_ENABLE_STATIC_EXECUTABLE}" "NOT BUILD_SHARED_LIBS" OFF)
if (TENZIR_ENABLE_STATIC_EXECUTABLE)
  # Not desired on darwin.
  if ("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
    # This works with clang as well, so we don't need an extra condition here.
    target_link_libraries(libtenzir_internal
                          INTERFACE -static-libgcc -static-libstdc++ -static)
  endif ()
endif ()

if (TENZIR_ENABLE_STATIC_EXECUTABLE AND BUILD_SHARED_LIBS)
  message(FATAL_ERROR "Cannot create static binary with dynamic libraries")
endif ()

if (TENZIR_ENABLE_RELOCATABLE_INSTALLATIONS)
  # Note that we cannot use the target property INSTALL_RPATH on
  # libtenzir_internal, because it is an interface library. We should probably fix
  # this by making the config header part of libtenzir_internal so it is not just
  # an interface library.
  if (NOT TENZIR_ENABLE_STATIC_EXECUTABLE)
    if (APPLE)
      list(PREPEND CMAKE_INSTALL_RPATH
           "@executable_path/../${CMAKE_INSTALL_LIBDIR}")
    else ()
      list(PREPEND CMAKE_INSTALL_RPATH "$ORIGIN/../${CMAKE_INSTALL_LIBDIR}")
    endif ()
  endif ()

  # For relocatable builds we need to find the relative difference between the
  # install prefix and the library location, or the binary location for static
  # Tenzir binaries.
  if (TENZIR_ENABLE_STATIC_EXECUTABLE)
    set(_libdir "${CMAKE_INSTALL_FULL_BINDIR}")
  else ()
    set(_libdir "${CMAKE_INSTALL_FULL_LIBDIR}")
  endif ()
  file(RELATIVE_PATH TENZIR_LIBDIR_TO_PREFIX "${_libdir}"
       "${CMAKE_INSTALL_PREFIX}")
  unset(_libdir)
endif ()

# -- sanitizers ----------------------------------------------------------------

# Address Sanitizer
set(_tenzir_build_types_with_default_asan "Debug")
if (NOT TENZIR_ENABLE_STATIC_EXECUTABLE
    AND CMAKE_BUILD_TYPE IN_LIST _tenzir_build_types_with_default_asan)
  set(_TENZIR_ENABLE_ASAN_DEFAULT ON)
else ()
  set(_TENZIR_ENABLE_ASAN_DEFAULT OFF)
endif ()

option(TENZIR_ENABLE_ASAN
       "Insert pointer and reference checks into the generated binaries"
       "${_TENZIR_ENABLE_ASAN_DEFAULT}")

unset(_TENZIR_ENABLE_ASAN_DEFAULT)

if (TENZIR_ENABLE_ASAN)
  # We need to enable the address sanitizer both globally and for the
  # libtenzir_internal targets in order to build (1) the bundled CAF with it and
  # (2) build external plugins with it.
  add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address)
  target_compile_options(libtenzir_internal INTERFACE -fsanitize=address
                                                      -fno-omit-frame-pointer)
  target_link_libraries(libtenzir_internal INTERFACE -fsanitize=address)
endif ()

# Undefined Behavior Sanitizer
option(TENZIR_ENABLE_UBSAN
       "Add run-time checks for undefined behavior into the generated binaries"
       OFF)
if (TENZIR_ENABLE_UBSAN)
  add_compile_options(-fsanitize=undefined)
  add_link_options(-fsanitize=undefined)
  target_compile_options(libtenzir_internal INTERFACE -fsanitize=undefined)
  target_link_libraries(libtenzir_internal INTERFACE -fsanitize=undefined)
endif ()

# -- allocator -----------------------------------------------------------------

set(TENZIR_ALLOCATOR_DEFAULT "system")
if (TENZIR_ENABLE_ASAN)
  set(TENZIR_ALLOCATOR_DEFAULT "none")
endif ()

set(TENZIR_ALLOCATOR
    ${TENZIR_ALLOCATOR_DEFAULT}
    CACHE
      STRING
      "Configure the allocator.\
Possible options:\
  * `none`: Tenzir's custom allocator API is not used. This means using the system allocator for most things and arrow uses its default alloctor\
  * `jemalloc`: jemalloc is used as an allocator for everything (arrow, operator new, malloc). You can enable stats tracking.\
  * `mimalloc`: mimalloc is used as an allocator for everything (arrow, operator new, malloc). You can enable stats tracking.\
  * `system`: the system allocator is ussed for everything (arrow, operator new, malloc). You can enable stats tracking.\
  * `runtime`: The allocator can be selected via an environment variable at startup. You can enable stats tracking.\
\
Environment variables:\
\
* `TENZIR_ALLOC=string`: (requires runtime allocator) Selects the allocator for the component. Value must be one of `jemalloc`, `mimalloc`, or `system`\
* `TENZIR_ALLOC_STATS=true`: (requires allocator not being disabled). Turns on stats counting for the component \
\
There also exist specific per-component variables, which are simply suffixed by _<COMPONENT>:\
* `ARROW`: All allocations made for arrow buffers only. Does not include things like the structure or arrow types.
* `CPP`: All allocations made through any `operator new` (does include std::allocator by definition)
* `C`: All allocations made through C or POSIX allocation functions.")

if (TENZIR_ENABLE_ASAN AND NOT TENZIR_ALLOCATOR STREQUAL "none")
  message(
    FATAL_ERROR "Allocator selection is not supported with address sanitizer.")
endif ()

if (TENZIR_ALLOCATOR STREQUAL "none")
  set(TENZIR_SELECT_ALLOCATOR "TENZIR_SELECT_ALLOCATOR_NONE")
elseif (TENZIR_ALLOCATOR STREQUAL "jemalloc")
  set(TENZIR_SELECT_ALLOCATOR "TENZIR_SELECT_ALLOCATOR_JEMALLOC")
elseif (TENZIR_ALLOCATOR STREQUAL "mimalloc")
  set(TENZIR_SELECT_ALLOCATOR "TENZIR_SELECT_ALLOCATOR_MIMALLOC")
elseif (TENZIR_ALLOCATOR STREQUAL "system")
  set(TENZIR_SELECT_ALLOCATOR "TENZIR_SELECT_ALLOCATOR_SYSTEM")
elseif (TENZIR_ALLOCATOR STREQUAL "runtime")
  set(TENZIR_SELECT_ALLOCATOR "TENZIR_SELECT_ALLOCATOR_RUNTIME")
else ()
  message(FATAL_ERROR "unknown TENZIR_ALLOCATOR")
endif ()

add_variable_to_summary("TENZIR_ALLOCATOR")

# -- developer mode ------------------------------------------------------------

option(TENZIR_ENABLE_CLANG_TIDY "Run clang-tidy during build" OFF)
set(TENZIR_CLANG_TIDY_ARGS
    "clang-tidy;--config-file=${PROJECT_SOURCE_DIR}/.clang-tidy")

option(TENZIR_ENABLE_CODE_COVERAGE "Add code coverage targets" OFF)
if (TENZIR_ENABLE_CODE_COVERAGE)
  if (NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    message(
      WARNING
        "Code coverage reports may be inaccurate for compilers other than gcc")
  endif ()
  set(CODE_COVERAGE "${TENZIR_ENABLE_CODE_COVERAGE}")
  include(CodeCoverage)
  mark_as_advanced(CODE_COVERAGE)
  install(CODE "\
    cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)
    message(FATAL_ERROR \"Builds with code coverage must not be installed.\")")
endif ()

TenzirTargetEnableTooling(libtenzir_internal INTERFACE)

# Add a custom executable that runs the CMake build targets 'test' and
# 'integration' so we can instrument them toether for combined code coverage.
if (TENZIR_ENABLE_CODE_COVERAGE)
  configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/all_tests.c.in"
                 "${CMAKE_CURRENT_BINARY_DIR}/cmake/all_tests.c" @ONLY)
  add_executable(all-tests "${CMAKE_CURRENT_BINARY_DIR}/cmake/all_tests.c")
  TenzirTargetEnableTooling(all-tests AUTO)
endif ()

# Build Tenzir in developer mode. This is enabled by default when not building
# Tenzir as a subproject. The developer mode contains CCache support and many
# other niceties.
option(TENZIR_ENABLE_DEVELOPER_MODE
       "Enables build settings for a nicer development environment"
       "${TENZIR_IS_NOT_SUBPROJECT}")

if (TENZIR_ENABLE_DEVELOPER_MODE)
  # Support tools like clang-tidy by creating a compilation database and
  # copying it to the project root.
  TenzirExportCompileCommands(libtenzir_internal)

  # Force colored output for the Ninja generator
  if ("${CMAKE_GENERATOR}" MATCHES "Ninja")
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
      target_compile_options(libtenzir_internal
                             INTERFACE -fdiagnostics-color=always)
    elseif ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
      target_compile_options(libtenzir_internal INTERFACE -fcolor-diagnostics)
    endif ()
  endif ()

  # Keep make output sane
  set(CMAKE_VERBOSE_MAKEFILE
      OFF
      CACHE STRING "Show all outputs including compiler lines." FORCE)

  # Enable CAF's compile-time type ID checks.
  target_compile_definitions(libtenzir_internal
                             INTERFACE CAF_ENABLE_TYPE_ID_CHECKS)
endif ()

if (TENZIR_ENABLE_DEVELOPER_MODE OR TENZIR_ENABLE_BUILDID)
  # Relocate debug paths to a common prefix for CCache users that work from
  # multiple worktrees.
  # The debug paths affect the build-id, we rewrite them to get a more
  # reproducible build.
  target_compile_options(
    libtenzir_internal
    INTERFACE "-fdebug-prefix-map=${CMAKE_CURRENT_SOURCE_DIR}=.")
endif ()

# -- additional warnings -------------------------------------------------------

option(TENZIR_ENABLE_MORE_WARNINGS "Enable addditional warnings" OFF)
if (TENZIR_ENABLE_MORE_WARNINGS)
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(
      libtenzir_internal
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
      libtenzir_internal
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

set(CLANG_MINIMUM_VERSION 13.0)
set(APPLE_CLANG_MINIMUM_VERSION 14.0)
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

# Disable module scanning.
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

# -- schemas -------------------------------------------------------------------

option(TENZIR_ENABLE_BUNDLED_SCHEMAS "Install bundled schemas with Tenzir" ON)
if (TENZIR_ENABLE_RELOCATABLE_INSTALLATIONS)
  add_custom_target(
    tenzir-schema
    COMMAND ${CMAKE_COMMAND} -E remove_directory
            "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_DATADIR}/tenzir/schema/"
    COMMAND
      ${CMAKE_COMMAND} -E copy_directory "${CMAKE_CURRENT_SOURCE_DIR}/schema"
      "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_DATADIR}/tenzir/schema/"
    COMMENT "Copying schema directory")
endif ()

if (TENZIR_ENABLE_BUNDLED_SCHEMAS)
  install(
    DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/schema"
    DESTINATION "${CMAKE_INSTALL_DATADIR}/tenzir"
    COMPONENT Runtime)
endif ()

# -- manpages ------------------------------------------------------------------

option(TENZIR_ENABLE_MANPAGES "Generate manpages for binaries" ON)
if (TENZIR_ENABLE_MANPAGES)
  find_package(Pandoc REQUIRED)
endif ()

# -- caf -----------------------------------------------------------------------

# The CAF dependency is loaded project-wide because both libtenzir and
# libtenzir_test need it.

# CAF::openssl needs OpenSSL, but CAFConfig.cmake does not pull it in.
find_package(OpenSSL REQUIRED)

set(TENZIR_CAF_LOG_LEVEL
    "WARNING"
    CACHE STRING "")

if (TENZIR_ENABLE_DEVELOPER_MODE
    AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/libtenzir/aux/caf/CMakeLists.txt"
    AND NOT CAF_ROOT_DIR)
  set(_tenzir_enable_bundled_caf_default ON)
else ()
  set(_tenzir_enable_bundled_caf_default OFF)
endif ()
option(TENZIR_ENABLE_BUNDLED_CAF "Use the CAF submodule"
       "${_tenzir_enable_bundled_caf_default}")
if (NOT TENZIR_ENABLE_BUNDLED_CAF)
  # Try to find the required CAF components first...
  find_package(
    CAF
    COMPONENTS core io net test openssl
    REQUIRED)
  if (NOT ${CAF_VERSION} VERSION_GREATER_EQUAL 1.0.2)
    message(
      FATAL_ERROR "Failed to find CAF >= 1.0.2 version (found ${CAF_VERSION})")
  endif ()
  string(
    APPEND
    TENZIR_FIND_DEPENDENCY_LIST
    "\nfind_package(CAF ${CAF_VERSION} COMPONENTS core io test openssl REQUIRED)"
  )
  dependency_summary("CAF" CAF::core "Dependencies")
else ()
  # Use bundled CAF.
  if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/libtenzir/aux/caf/CMakeLists.txt")
    message(
      FATAL_ERROR
        "CAF not found, either use -DCAF_ROOT_DIR=... or initialize the
        libtenzir/aux/caf submodule")
  else ()
    set(TENZIR_ENABLE_BUNDLED_CAF ON)
    set(CAF_LOG_LEVEL
        "${TENZIR_CAF_LOG_LEVEL}"
        CACHE STRING "")
    set(CAF_ENABLE_EXAMPLES
        OFF
        CACHE BOOL "")
    set(CAF_ENABLE_TESTING
        OFF
        CACHE BOOL "")
    set(CAF_ENABLE_TOOLS
        OFF
        CACHE BOOL "")
    set(CAF_ENABLE_OPENSSL
        ON
        CACHE BOOL "")
    if (TENZIR_ENABLE_ASSERTIONS)
      set(CAF_ENABLE_RUNTIME_CHECKS
          ON
          CACHE BOOL "")
    endif ()
    # add_subdirectory libtenzir/aux/caf checks if compiler supports the c++ 17. This check fails and the workaround
    # can be removed once CAF cmake changes to use set(CMAKE_CXX_STANDARD) instead of it's own check
    set(ORIGINAL_CXX_STANDARD ${CMAKE_CXX_STANDARD})
    set(CMAKE_CXX_STANDARD 23)
    if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.25")
      add_subdirectory(libtenzir/aux/caf SYSTEM)
    else ()
      add_subdirectory(libtenzir/aux/caf)
    endif ()
    set(CMAKE_CXX_STANDARD ${ORIGINAL_CXX_STANDARD})
    TenzirSystemizeTarget(libcaf_core)
    set_target_properties(libcaf_core PROPERTIES EXPORT_NAME core)
    target_compile_features(libcaf_core INTERFACE cxx_std_17)
    target_compile_options(
      libcaf_core
      PRIVATE
        -Wno-maybe-uninitialized
        -Wno-unqualified-std-cast-call
        -Wno-unknown-warning-option
        $<$<CXX_COMPILER_ID:Clang,AppleClang>:-Wno-deprecated-declarations>
        $<$<CXX_COMPILER_ID:GNU>:-Wno-deprecated>)
    TenzirSystemizeTarget(libcaf_io)
    set_target_properties(libcaf_io PROPERTIES EXPORT_NAME io)
    target_compile_options(
      libcaf_io PRIVATE -Wno-maybe-uninitialized -Wno-unqualified-std-cast-call
                        -Wno-unknown-warning-option)
    TenzirSystemizeTarget(libcaf_net)
    set_target_properties(libcaf_net PROPERTIES EXPORT_NAME net)
    target_compile_options(
      libcaf_io PRIVATE -Wno-maybe-uninitialized -Wno-unqualified-std-cast-call
                        -Wno-unknown-warning-option)
    if (NOT TARGET libcaf_openssl)
      string(
        JOIN " " err_message
        "Unable to find the bundled CAF's OpenSSL module; consider setting "
        "OPENSSL_ROOT_DIR to the install prefix of your OpenSSL installation.")
      message(FATAL_ERROR "${err_message}")
    endif ()
    target_include_directories(
      libcaf_openssl
      PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/libtenzir/aux/caf/libcaf_openssl>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    TenzirSystemizeTarget(libcaf_openssl)
    set_target_properties(libcaf_openssl PROPERTIES EXPORT_NAME openssl)
    TenzirSystemizeTarget(libcaf_test)
    set_target_properties(libcaf_test PROPERTIES EXPORT_NAME test)
    mark_as_advanced(caf_build_header_path)
    string(APPEND TENZIR_EXTRA_TARGETS_FILES
           "\ninclude(\"\${CMAKE_CURRENT_LIST_DIR}/../CAF/CAFTargets.cmake\")"
           "\nmark_as_advanced(caf_build_header_path)")
    set(CAF_FOUND true)
  endif ()
  # Make bundled CAF available for component-based CPack installations.
  install(TARGETS libcaf_core libcaf_openssl libcaf_io libcaf_net
          COMPONENT Runtime)
  install(TARGETS libcaf_test COMPONENT Development)
  # Figure out whether we point to a build directory or a prefix.
  dependency_summary("CAF" "${CMAKE_CURRENT_SOURCE_DIR}/libtenzir/aux/caf"
                     "Dependencies")
endif ()

if (TENZIR_ENABLE_RELOCATABLE_INSTALLATIONS
    AND BUILD_SHARED_LIBS
    AND CAF_LIBRARY_CORE)
  # Copy CAF libraries to installation directory
  get_filename_component(CAF_LIBDIR ${CAF_LIBRARY_CORE} PATH)
  file(GLOB CAF_INSTALLED_LIBRARIES "${CAF_LIBDIR}/libcaf*.so")
  install(
    FILES ${CAF_INSTALLED_LIBRARIES}
    DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    COMPONENT Runtime)
endif ()

# -- python bindings ----------------------------------------------------------

option(TENZIR_ENABLE_PYTHON_BINDINGS "Build Python bindings (requires uv)" ON)

set(_tenzir_enable_bundled_uv_default "ON")
if (DEFINED ENV{TENZIR_ENABLE_BUNDLED_UV})
  set(_tenzir_enable_bundled_uv_default $ENV{TENZIR_ENABLE_BUNDLED_UV})
endif ()

cmake_dependent_option(
  TENZIR_ENABLE_BUNDLED_UV "Install a statically linked copy of uv"
  "${_tenzir_enable_bundled_uv_default}" "TENZIR_ENABLE_PYTHON_BINDINGS" OFF)

if (TENZIR_ENABLE_PYTHON_BINDINGS)
  set(TENZIR_PYTHON_BINARY_DIR "${CMAKE_BINARY_DIR}/share/tenzir/python")
  if (TENZIR_PYTHON_DEPENDENCY_WHEELS)
    add_custom_command(
      OUTPUT "${TENZIR_PYTHON_BINARY_DIR}/.stamp"
      COMMAND ${CMAKE_COMMAND} -E remove_directory "${TENZIR_PYTHON_BINARY_DIR}"
      COMMAND ${CMAKE_COMMAND} -E make_directory "${TENZIR_PYTHON_BINARY_DIR}"
      COMMAND ${CMAKE_COMMAND} -E copy_directory
              "${TENZIR_PYTHON_DEPENDENCY_WHEELS}" "${TENZIR_PYTHON_BINARY_DIR}"
      COMMAND ${CMAKE_COMMAND} -E touch "${TENZIR_PYTHON_BINARY_DIR}/.stamp"
      COMMENT "Copying python operator wheels"
      VERBATIM)
  else ()
    find_program(UV_PATH uv)
    if (NOT UV_PATH)
      message(
        FATAL_ERROR
          "Cannot find 'uv' in PATH, which is required for building Tenzir Python bindings"
      )
    endif ()
    find_package(
      Python3
      COMPONENTS Interpreter
      REQUIRED)
    set(TENZIR_PYTHON_WORKSPACE "${CMAKE_CURRENT_SOURCE_DIR}/python")
    file(GLOB_RECURSE TENZIR_PYTHON_COMMON_DEPENDENCIES CONFIGURE_DEPENDS
         "${TENZIR_PYTHON_WORKSPACE}/tenzir-common/*")
    file(GLOB_RECURSE TENZIR_PYTHON_OPERATOR_DEPENDENCIES CONFIGURE_DEPENDS
         "${TENZIR_PYTHON_WORKSPACE}/tenzir-operator/*")
    set(TENZIR_PYTHON_WORKSPACE_DENPENDENCIES
        "${TENZIR_PYTHON_WORKSPACE}/pyproject.toml"
        "${TENZIR_PYTHON_WORKSPACE}/uv.lock"
        "${TENZIR_PYTHON_WORKSPACE}/README.md"
        "${TENZIR_PYTHON_WORKSPACE}/uv-source-info.json")
    list(FILTER TENZIR_PYTHON_WORKSPACE_DENPENDENCIES INCLUDE REGEX ".*")
    add_custom_command(
      OUTPUT "${TENZIR_PYTHON_BINARY_DIR}/.stamp"
      COMMAND ${CMAKE_COMMAND} -E remove_directory "${TENZIR_PYTHON_BINARY_DIR}"
      COMMAND ${CMAKE_COMMAND} -E make_directory "${TENZIR_PYTHON_BINARY_DIR}"
      COMMAND
        ${CMAKE_COMMAND} -E env "UV_CACHE_DIR=${CMAKE_BINARY_DIR}/uv-cache"
        "TENZIR_SKIP_NIX_BUILD=1" "TENZIR_REPO_ROOT=${CMAKE_SOURCE_DIR}"
        "UV_PYTHON=${Python3_EXECUTABLE}" -- "${UV_PATH}" build -q --wheel
        --package tenzir-common --out-dir "${TENZIR_PYTHON_BINARY_DIR}"
      COMMAND
        ${CMAKE_COMMAND} -E env "UV_CACHE_DIR=${CMAKE_BINARY_DIR}/uv-cache"
        "TENZIR_SKIP_NIX_BUILD=1" "TENZIR_REPO_ROOT=${CMAKE_SOURCE_DIR}"
        "UV_PYTHON=${Python3_EXECUTABLE}" -- "${UV_PATH}" build -q --wheel
        --package tenzir-operator --out-dir "${TENZIR_PYTHON_BINARY_DIR}"
      COMMAND ${CMAKE_COMMAND} -E touch "${TENZIR_PYTHON_BINARY_DIR}/.stamp"
      WORKING_DIRECTORY "${TENZIR_PYTHON_WORKSPACE}"
      DEPENDS "${TENZIR_PYTHON_REQUIREMENTS_FILE}"
              ${TENZIR_PYTHON_COMMON_DEPENDENCIES}
              ${TENZIR_PYTHON_CORE_DEPENDENCIES}
              ${TENZIR_PYTHON_OPERATOR_DEPENDENCIES}
      COMMENT "Building python operator wheelhouse"
      VERBATIM)
  endif ()
  add_custom_target(tenzir-python-wheels
                    DEPENDS "${TENZIR_PYTHON_BINARY_DIR}/.stamp")
  add_custom_target(tenzir-python-wheel DEPENDS tenzir-python-wheels)
  install(
    DIRECTORY "${TENZIR_PYTHON_BINARY_DIR}"
    DESTINATION "${CMAKE_INSTALL_DATADIR}/tenzir"
    COMPONENT Runtime)
  if (TENZIR_ENABLE_BUNDLED_UV)
    set(build_libexecdir "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBEXECDIR}")
    if (NOT TENZIR_UV_PATH)
      file(READ "${CMAKE_CURRENT_LIST_DIR}/python/uv-source-info.json"
           uv_source_info)
      set(UV_SYSTEM_arm64_Darwin "aarch64-darwin")
      set(UV_SYSTEM_aarch64_Linux "aarch64-linux")
      set(UV_SYSTEM_x86_64_Darwin "x86_64-darwin")
      set(UV_SYSTEM_x86_64_Linux "x86_64-linux")
      string(
        JSON uv_source GET "${uv_source_info}"
        "${UV_SYSTEM_${CMAKE_HOST_SYSTEM_PROCESSOR}_${CMAKE_HOST_SYSTEM_NAME}}")
      string(JSON uv_url GET "${uv_source}" url)
      string(JSON uv_sha256 GET "${uv_source}" sha256)
      get_filename_component(uv_stem "${uv_url}" NAME_WE)
      file(DOWNLOAD "${uv_url}" "${CMAKE_CURRENT_BINARY_DIR}/uv.tar.gz"
           EXPECTED_HASH "SHA256=${uv_sha256}")
      file(ARCHIVE_EXTRACT INPUT "${CMAKE_CURRENT_BINARY_DIR}/uv.tar.gz"
           DESTINATION "${CMAKE_CURRENT_BINARY_DIR}" VERBOSE)
      set(TENZIR_UV_PATH "${CMAKE_CURRENT_BINARY_DIR}/${uv_stem}/uv")
    endif ()
    file(MAKE_DIRECTORY "${build_libexecdir}")
    file(COPY_FILE "${TENZIR_UV_PATH}" "${build_libexecdir}/uv")
    install(
      PROGRAMS "${TENZIR_UV_PATH}"
      DESTINATION "${CMAKE_INSTALL_LIBEXECDIR}"
      COMPONENT Runtime)
  else ()
    set(TENZIR_UV_PATH "uv")
  endif ()
endif ()

# -- add subdirectories --------------------------------------------------------

add_subdirectory(libtenzir)
add_subdirectory(libtenzir_test)
add_subdirectory(tenzir)

if (BUILD_TESTING)
  add_custom_target(
    unit-tests
    COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process --output-on-failure
            --label-regex unit --build-config $<CONFIG>
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    USES_TERMINAL VERBATIM)
  add_dependencies(unit-tests tenzir)
endif ()

# -- cmake export/config installations -----------------------------------------

export(
  EXPORT TenzirTargets
  FILE TenzirTargets.cmake
  NAMESPACE tenzir::)

install(
  EXPORT TenzirTargets
  DESTINATION "${TENZIR_INSTALL_CMAKEDIR}/tenzir"
  NAMESPACE tenzir::
  COMPONENT Development)

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/TenzirConfigVersion.cmake"
  VERSION "${TENZIR_VERSION}"
  COMPATIBILITY ExactVersion)

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TenzirConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/TenzirConfig.cmake"
  INSTALL_DESTINATION "${TENZIR_INSTALL_CMAKEDIR}/tenzir")

configure_file("${PROJECT_SOURCE_DIR}/cmake/TenzirMacDependencyPaths.cmake"
               ${CMAKE_BINARY_DIR} COPYONLY)
configure_file("${PROJECT_SOURCE_DIR}/cmake/TenzirRegisterPlugin.cmake"
               ${CMAKE_BINARY_DIR} COPYONLY)

install(
  FILES "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TenzirRegisterPlugin.cmake"
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TenzirMacDependencyPaths.cmake"
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TenzirUtilities.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/TenzirConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/TenzirConfigVersion.cmake"
  DESTINATION "${TENZIR_INSTALL_CMAKEDIR}/tenzir"
  COMPONENT Development)

# -- scripts -------------------------------------------------------------------

option(TENZIR_ENABLE_TENZIR_DF_PERCENT
       "Install tenzir-df-percent.sh with Tenzir" ON)
if (TENZIR_ENABLE_TENZIR_DF_PERCENT)
  install(
    PROGRAMS "${CMAKE_CURRENT_SOURCE_DIR}/scripts/tenzir-df-percent.sh"
    DESTINATION "${CMAKE_INSTALL_LIBEXECDIR}"
    COMPONENT Runtime)
endif ()

# -- docdir --------------------------------------------------------------------

# Install CHANGELOG, LICENSE, README, and VERSIONING. This can intentionally not
# be disabled.
install(
  FILES "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE"
        "${CMAKE_CURRENT_SOURCE_DIR}/VERSIONING.md"
        "${CMAKE_CURRENT_SOURCE_DIR}/README.md"
  DESTINATION "${CMAKE_INSTALL_DOCDIR}"
  COMPONENT Runtime)

# -- packaging -----------------------------------------------------------------

include(TenzirPackage)
