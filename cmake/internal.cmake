# -- internal target -----------------------------------------------------------

# Create the internal target that is used for option/property propagation.
add_library(libvast_internal INTERFACE)
set_target_properties(libvast_internal PROPERTIES EXPORT_NAME internal)
add_library(vast::internal ALIAS libvast_internal)
install(
  TARGETS libvast_internal
  EXPORT libVASTTargets
  COMPONENT Development)

# Require standard C++20.
target_compile_features(libvast_internal INTERFACE cxx_std_20)

# Enable coroutines for GCC < 11.
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION
                                             VERSION_LESS 11)
  target_compile_options(libvast_internal INTERFACE -fcoroutines)
endif ()

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
            -Werror=odr
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

option(VAST_ENABLE_ASSERTIONS_CHEAP "Enable cheap assertions" ON)
add_feature_info("VAST_ENABLE_ASSERTIONS_CHEAP" VAST_ENABLE_ASSERTIONS_CHEAP
                 "enable assertions that are cheap to check.")

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

# -- sanitizers ----------------------------------------------------------------

# Address Sanitizer
set(_vast_build_types_with_default_asan "Debug;CI")
if (NOT VAST_ENABLE_STATIC_EXECUTABLE AND CMAKE_BUILD_TYPE IN_LIST
                                          _vast_build_types_with_default_asan)
  set(_VAST_ENABLE_ASAN_DEFAULT ON)
else ()
  set(_VAST_ENABLE_ASAN_DEFAULT OFF)
endif ()

option(VAST_ENABLE_ASAN
       "Insert pointer and reference checks into the generated binaries"
       "${_VAST_ENABLE_ASAN_DEFAULT}")
add_feature_info(
  "VAST_ENABLE_ASAN" VAST_ENABLE_ASAN
  "inserts pointer and reference checks into the generated binaries.")

unset(_VAST_ENABLE_ASAN_DEFAULT)

if (VAST_ENABLE_ASAN)
  # We need to enable the address sanitizer both globally and for the
  # libvast_internal targets in order to build (1) the bundled CAF with it and
  # (2) build external plugins with it.
  add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address)
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
  add_compile_options(-fsanitize=undefined)
  add_link_options(-fsanitize=undefined)
  target_compile_options(libvast_internal INTERFACE -fsanitize=undefined)
  target_link_libraries(libvast_internal INTERFACE -fsanitize=undefined)
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
