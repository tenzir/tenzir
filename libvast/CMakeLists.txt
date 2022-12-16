# -- bundled dependencies ------------------------------------------------------

# TODO: VAST bundles robin-map in its aux directory, because we need at least
# 0.6.2, which is not widely available on package managers. We should
# investigate switching to a different map, e.g., from folly or abseil.

option(VAST_ENABLE_BUNDLED_ROBIN_MAP "Always use the bundled tsl-robin-map" OFF)
add_feature_info("VAST_ENABLE_BUNDLED_ROBIN_MAP" VAST_ENABLE_BUNDLED_ROBIN_MAP
                 "always use the bundled tsl-robin-map.")
if (NOT VAST_ENABLE_BUNDLED_ROBIN_MAP)
  find_package(tsl-robin-map QUIET)
  if (tsl-robin-map_FOUND)
    string(APPEND VAST_FIND_DEPENDENCY_LIST
           "\nfind_package(tsl-robin-map 0.6.2 REQUIRED)")
    dependency_summary("robin-map" tsl::robin_map "Dependencies")
  endif ()
endif ()
if (NOT tsl-robin-map_FOUND)
  if (NOT VAST_ENABLE_BUNDLED_ROBIN_MAP)
    message(
      STATUS
        "Cannot find installed tsl-robin-map; falling back to bundled version")
  endif ()
  add_subdirectory(aux/robin-map)
  dependency_summary("robin-map" "${CMAKE_CURRENT_SOURCE_DIR}/aux/robin-map"
                     "Dependencies")
  export(
    EXPORT tsl-robin-mapTargets
    FILE tsl-robin-mapTargets.cmake
    NAMESPACE tsl::)
  add_custom_target(
    tsl-robin-map-targets-link
    COMMAND
      ${CMAKE_COMMAND} -E create_symlink
      "${CMAKE_CURRENT_BINARY_DIR}/tsl-robin-mapTargets.cmake"
      "${CMAKE_BINARY_DIR}/tsl-robin-mapTargets.cmake"
    COMMENT "Linking tsl-robin-map targets")
  install(
    EXPORT tsl-robin-mapTargets
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vast"
    NAMESPACE tsl::
    COMPONENT Development)
  string(APPEND VAST_EXTRA_TARGETS_FILES
         "\ninclude(\"\${CMAKE_CURRENT_LIST_DIR}/tsl-robin-mapTargets.cmake\")")
  add_dependencies(robin_map tsl-robin-map-targets-link)
endif ()

# -- flatbuffers ---------------------------------------------------------------

file(GLOB flatbuffers_schemas CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/fbs/*.fbs"
     "${CMAKE_CURRENT_SOURCE_DIR}/fbs/legacy/*.fbs")
list(SORT flatbuffers_schemas)

VASTCompileFlatBuffers(
  TARGET libvast-fbs
  SCHEMAS ${flatbuffers_schemas}
  INCLUDE_DIRECTORY "vast/fbs/")

# -- arrow ---------------------------------------------------------------------

find_package(Arrow REQUIRED CONFIG)
if (Arrow_VERSION_MAJOR LESS 8)
  message(
    FATAL_ERROR
      "VAST requires at least Arrow version 8.0, but found ${ARROW_VERSION}")
endif ()
mark_as_advanced(
  BROTLI_COMMON_LIBRARY
  BROTLI_DEC_LIBRARY
  BROTLI_ENC_LIBRARY
  LZ4_LIB
  Snappy_INCLUDE_DIR
  Snappy_LIB
  ZSTD_LIB)
string(
  APPEND
  VAST_FIND_DEPENDENCY_LIST
  "\nfind_package(Arrow REQUIRED CONFIG)"
  "\nmark_as_advanced("
  "\n BROTLI_COMMON_LIBRARY"
  "\n BROTLI_DEC_LIBRARY"
  "\n BROTLI_ENC_LIBRARY"
  "\n LZ4_LIB"
  "\n Snappy_INCLUDE_DIR"
  "\n Snappy_LIB"
  "\n ZSTD_LIB)")
if (BUILD_SHARED_LIBS)
  set(ARROW_LIBRARY arrow_shared)
else ()
  set(ARROW_LIBRARY arrow_static)
endif ()

# -- libsystemd ----------------------------------------------------------------

option(VAST_ENABLE_JOURNALD_LOGGING
       "Build with support for native journald log output" OFF)
add_feature_info("VAST_ENABLE_JOURNALD_LOGGING" VAST_ENABLE_JOURNALD_LOGGING
                 "build with support for native journald log output.")

# -- log level -----------------------------------------------------------------

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

# -- caf -----------------------------------------------------------------------

function (VASTSystemizeTarget tgt)
  message(STATUS "Treating ${tgt} as a system dependency")
  get_target_property(_includes ${tgt} INTERFACE_INCLUDE_DIRECTORIES)
  set_property(
    TARGET ${tgt}
    APPEND
    PROPERTY INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${_includes}")
endfunction ()

# TODO: Require CAF to be installed.

if (VAST_ENABLE_DEVELOPER_MODE
    AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/aux/caf/CMakeLists.txt"
    AND NOT CAF_ROOT_DIR)
  set(VAST_ENABLE_BUNDLED_CAF_DEFAULT ON)
else ()
  set(VAST_ENABLE_BUNDLED_CAF_DEFAULT OFF)
endif ()
option(VAST_ENABLE_BUNDLED_CAF "Use the CAF submodule"
       "${VAST_ENABLE_BUNDLED_CAF_DEFAULT}")
add_feature_info("VAST_ENABLE_BUNDLED_CAF" VAST_ENABLE_BUNDLED_CAF
                 "use the CAF submodule.")
if (NOT VAST_ENABLE_BUNDLED_CAF)
  # Try to find the required CAF components first...
  provide_find_module(CAF)
  find_package(
    CAF
    COMPONENTS core io test openssl
    REQUIRED)
  if (NOT ${CAF_VERSION} VERSION_GREATER_EQUAL 0.18.6)
    message(
      FATAL_ERROR "Failed to find CAF >= 0.18.6 version (found ${CAF_VERSION})")
  endif ()
  string(
    APPEND
    VAST_FIND_DEPENDENCY_LIST
    "\nfind_package(CAF ${CAF_VERSION} COMPONENTS core io test openssl REQUIRED)"
  )
else ()
  # Use bundled CAF.
  if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/aux/caf/CMakeLists.txt")
    message(
      FATAL_ERROR
        "CAF not found, either use -DCAF_ROOT_DIR=... or initialize the libvast/aux/caf submodule"
    )
  else ()
    set(VAST_ENABLE_BUNDLED_CAF ON)
    set(CAF_LOG_LEVEL ${VAST_CAF_LOG_LEVEL})
    set(CAF_ENABLE_EXAMPLES OFF)
    set(CAF_ENABLE_TESTING OFF)
    set(CAF_ENABLE_TOOLS OFF)
    set(CAF_ENABLE_OPENSSL ON)
    # add_subdirectory aux/caf checks if compiler supports the c++ 17. This check fails and the workaround
    # can be removed once CAF cmake changes to use set(CMAKE_CXX_STANDARD) instead of it's own check
    set(ORIGINAL_CXX_STANDARD ${CMAKE_CXX_STANDARD})
    set(CMAKE_CXX_STANDARD 17)
    add_subdirectory(aux/caf)
    set(CMAKE_CXX_STANDARD ${ORIGINAL_CXX_STANDARD})
    VASTSystemizeTarget(libcaf_core)
    set_target_properties(libcaf_core PROPERTIES EXPORT_NAME core)
    target_compile_features(libcaf_core INTERFACE cxx_std_17)
    target_compile_options(
      libcaf_core
      PRIVATE -Wno-maybe-uninitialized -Wno-unqualified-std-cast-call
              -Wno-unknown-warning-option)
    VASTSystemizeTarget(libcaf_io)
    set_target_properties(libcaf_io PROPERTIES EXPORT_NAME io)
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
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/aux/caf/libcaf_openssl>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    VASTSystemizeTarget(libcaf_openssl)
    set_target_properties(libcaf_openssl PROPERTIES EXPORT_NAME openssl)
    # Fix deprecation warnings for OpenSSL >= 3.0.
    target_compile_options(
      libcaf_openssl
      PRIVATE
        $<$<CXX_COMPILER_ID:Clang,AppleClang>:-Wno-deprecated-declarations>
        $<$<CXX_COMPILER_ID:GNU>:-Wno-deprecated>)
    VASTSystemizeTarget(libcaf_test)
    set_target_properties(libcaf_test PROPERTIES EXPORT_NAME test)
    mark_as_advanced(caf_build_header_path)
    string(APPEND VAST_EXTRA_TARGETS_FILES
           "\ninclude(\"\${CMAKE_CURRENT_LIST_DIR}/../CAF/CAFTargets.cmake\")"
           "\nmark_as_advanced(caf_build_header_path)")
    set(CAF_FOUND true)
  endif ()
endif ()

if (VAST_ENABLE_RELOCATABLE_INSTALLATIONS
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

# -- libvast -------------------------------------------------------------------

# The builtins. These special plugins are essentially part of the libvast
# library, but always loaded when linking against the libvast_builtins library.
file(GLOB_RECURSE libvast_builtins_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp")
foreach (source IN LISTS libvast_builtins_sources)
  set(has_register FALSE)
  file(STRINGS "${source}" lines)
  foreach (line IN LISTS lines)
    if ("${line}" MATCHES "^ *VAST_REGISTER_PLUGIN *\\(.+\\) *$")
      set(has_register TRUE)
      break()
    endif ()
  endforeach ()
  if (NOT "${has_register}")
    message(FATAL_ERROR "builtin ${source} does not register as a plugin")
  endif ()
endforeach ()

file(GLOB_RECURSE libvast_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
     "${CMAKE_CURRENT_SOURCE_DIR}/include/vast/*.hpp")

add_library(libvast ${libvast_sources})
VASTTargetEnableTooling(libvast)
add_library(vast::libvast ALIAS libvast)

add_library(libvast_builtins STATIC ${libvast_builtins_sources})
VASTTargetEnableTooling(libvast_builtins)
target_compile_definitions(libvast_builtins PRIVATE VAST_ENABLE_BUILTINS)
set_target_properties(libvast_builtins PROPERTIES OUTPUT_NAME vast_builtins)
add_library(vast::libvast_builtins ALIAS libvast_builtins)

target_link_libraries(libvast PRIVATE libvast_internal)
target_link_libraries(libvast_builtins PRIVATE libvast libvast_internal)

set_target_properties(libvast PROPERTIES INTERPROCEDURAL_OPTIMIZATION_RELEASE
                                         ON)

# Set the libvast SOVERSION to '<256 * (major + 7) + minor>.<patch>' to indicate
# that libvast is considered to have an unstable API and ABI. The +7 is a
# remnant of VAST using CalVer before 2022, with the last CalVer release having
# the SOVERSION 2022.12, and it enforces that v1.0.0 has a larger SOVERSION than
# the last CalVer release.
math(EXPR VAST_SOVERSION_MAJOR
     "256 * (${VAST_VERSION_MAJOR} + 7) + ${VAST_VERSION_MINOR}")
set(VAST_SOVERSION_MINOR "${VAST_VERSION_PATCH}")
set_target_properties(
  libvast PROPERTIES SOVERSION "${VAST_SOVERSION_MAJOR}.${VAST_SOVERSION_MINOR}"
                     OUTPUT_NAME vast)

target_include_directories(
  libvast
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

# Make dlopen and dlclose available if we're building a shared library.
if (BUILD_SHARED_LIBS)
  target_link_libraries(libvast PUBLIC ${CMAKE_DL_LIBS})
endif ()

# Link against CAF.
target_link_libraries(libvast PUBLIC CAF::core CAF::io CAF::openssl)
if (VAST_ENABLE_BUNDLED_CAF)
  # Figure out whether we point to a build directory or a prefix.
  if (NOT caf_dir)
    set(caf_dir ${CAF_INCLUDE_DIRS})
  endif ()
  dependency_summary("CAF" "${caf_dir}" "Dependencies")
else ()
  dependency_summary("CAF" CAF::core "Dependencies")
endif ()

# Link against libvast-fbs.
target_link_libraries(libvast PUBLIC libvast-fbs)

# Link against OpenSSL
find_package(OpenSSL REQUIRED)
target_link_libraries(libvast PUBLIC OpenSSL::SSL)
string(APPEND VAST_FIND_DEPENDENCY_LIST "\nfind_package(OpenSSL REQUIRED)")
dependency_summary("OpenSSL" OpenSSL::SSL "Dependencies")

# Link against yaml-cpp.
find_package(yaml-cpp 0.6.2 REQUIRED)
target_link_libraries(libvast PRIVATE yaml-cpp)
dependency_summary("yaml-cpp" yaml-cpp "Dependencies")

# Link against xxHash.
target_compile_definitions(
  libvast
  PUBLIC # Allow null pointer input when hashing data of length greater 0.
         # TODO: Remove this when requiring xxHash >= 0.8.1.
         XXH_ACCEPT_NULL_INPUT_POINTER=1)
find_package(xxhash REQUIRED)
provide_find_module(xxhash)
string(APPEND VAST_FIND_DEPENDENCY_LIST "\nfind_package(xxhash REQUIRED)")
target_link_libraries(libvast PUBLIC xxhash::xxhash_header_only)
dependency_summary("xxHash" xxhash::xxhash_header_only "Dependencies")

# Link against libsystemd.
if (VAST_ENABLE_JOURNALD_LOGGING)
  find_package(systemd REQUIRED)
  target_link_libraries(libvast PRIVATE systemd::systemd)
  dependency_summary("libsystemd" systemd::systemd "Dependencies")
endif ()

# Link against fmt.
target_compile_definitions(
  libvast
  PUBLIC
    # Forbid unsafe duration_cast usage.
    FMT_SAFE_DURATION_CAST
    # Make fmt::internal an alias for fmt::detail. Remove when requiring fmt >= 7.
    FMT_USE_INTERNAL)
find_package(fmt 5.2.1 REQUIRED)
string(APPEND VAST_FIND_DEPENDENCY_LIST "\nfind_package(fmt 5.2.1 REQUIRED)")
target_link_libraries(libvast PUBLIC fmt::fmt)
dependency_summary("fmt" fmt::fmt "Dependencies")

target_compile_definitions(libvast PUBLIC SPDLOG_FMT_EXTERNAL)
find_package(spdlog 1.5.0 REQUIRED)
string(APPEND VAST_FIND_DEPENDENCY_LIST "\nfind_package(spdlog 1.5.0 REQUIRED)")
target_link_libraries(libvast PUBLIC spdlog::spdlog)
dependency_summary("spdlog" spdlog::spdlog "Dependencies")

# Link against simdjson.
find_package(
  simdjson
  REQUIRED
  # simdjson 0.8.0 exports it package configuration file under the wrong name,
  # so we look for "simdjson-targets.cmake" as well.
  CONFIGS
  "simdjson-config.cmake"
  "simdjson-targets.cmake")
string(APPEND VAST_FIND_DEPENDENCY_LIST
       "\nfind_package(simdjson REQUIRED CONFIGS "
       "\"simdjson-config.cmake\" \"simdjson-targets.cmake\")")
target_link_libraries(libvast PUBLIC simdjson::simdjson)
dependency_summary("simdjson" simdjson::simdjson "Dependencies")

# Link against robin-map.
target_link_libraries(libvast PUBLIC tsl::robin_map)
# This is broken for the bundled robin-map, so we special-case it above.
# dependency_summary("robin-map" tsl::robin_map "Dependencies")

# Link against Apache Arrow.
target_link_libraries(libvast PUBLIC ${ARROW_LIBRARY})
dependency_summary("Apache Arrow" ${ARROW_LIBRARY} "Dependencies")

# Link against fast_float
option(VAST_ENABLE_BUNDLED_FASTFLOAT "Use the fast_float submodule" OFF)
add_feature_info("VAST_ENABLE_BUNDLED_FASTFLOAT" VAST_ENABLE_BUNDLED_FASTFLOAT
                 "use the fast_float submodule.")
if (NOT VAST_ENABLE_BUNDLED_FASTFLOAT)
  find_package(FastFloat 3.4.0 CONFIG)
endif ()
if (NOT FastFloat_FOUND)
  # Use bundled fast_float.
  if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/aux/fast_float/CMakeLists.txt")
    message(
      FATAL_ERROR
        "fast_float library not found, either use -FastFloat_DIR=... or"
        " initialize the libvast/aux/fast_float submodule")
  endif ()
  add_subdirectory(aux/fast_float)
  add_library(FastFloat::fast_float ALIAS fast_float)
  dependency_summary("fast_float" "${CMAKE_CURRENT_SOURCE_DIR}/aux/fast_float"
                     "Dependencies")
else ()
  dependency_summary("fast_float" FastFloat::fast_float "Dependencies")
endif ()
target_link_libraries(libvast PRIVATE FastFloat::fast_float)

# Link against a backtrace library.
option(VAST_ENABLE_BACKTRACE "Print a backtrace on unexpected termination" ON)
add_feature_info("VAST_ENABLE_BACKTRACE" VAST_ENABLE_BACKTRACE
                 "print a backtrace on unexpected termination.")
unset(VAST_ENABLE_LIBUNWIND)
unset(VAST_ENABLE_LIBBACKTRACE)
if (VAST_ENABLE_BACKTRACE)
  find_package(libunwind)
  if (libunwind_FOUND)
    target_link_libraries(libvast PRIVATE unwind::unwind)
    dependency_summary("libunwind" unwind::unwind "Dependencies")
    set(VAST_ENABLE_LIBUNWIND ON)
  else ()
    find_package(Backtrace)
    if (Backtrace_FOUND)
      target_include_directories(libvast
                                 PRIVATE ${Backtrace_INCLUDE_DIRECTORIES})
      target_link_libraries(libvast PRIVATE ${Backtrace_LIBRARIES})
      if ("${Backtrace_HEADER}" STREQUAL "backtrace.h")
        dependency_summary("libbacktrace" "${Backtrace_INCLUDE_DIR}"
                           "Dependencies")
        set(VAST_ENABLE_LIBBACKTRACE ON)
      elseif ("${Backtrace_HEADER}" STREQUAL "execinfo.h")
        dependency_summary("libexecinfo" "${Backtrace_INCLUDE_DIR}"
                           "Dependencies")
        set(VAST_ENABLE_LIBEXECINFO ON)
      else ()
        message(
          FATAL_ERROR "Found unexpected backtrace header ${Backtrace_HEADER}")
      endif ()
    endif ()
  endif ()
endif ()

# Configure options-dependent code files.
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/detail/installdirs.cpp.in"
               "${CMAKE_CURRENT_BINARY_DIR}/src/detail/installdirs.cpp" @ONLY)

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/include/vast/config.hpp.in"
               "${CMAKE_CURRENT_BINARY_DIR}/include/vast/config.hpp")

if (CMAKE_BUILD_TYPE)
  set(VAST_BUILD_TYPE "${CMAKE_BUILD_TYPE}")
elseif (CMAKE_CONFIGURATION_TYPES)
  list(JOIN CMAKE_CONFIGURATION_TYPES " " VAST_BUILD_TYPE)
  string(PREPEND VAST_BUILD_TYPE "Multi-Config: ")
else ()
  message(FATAL_ERROR "Failed to detect build or configuration types")
endif ()

file(
  WRITE "${CMAKE_CURRENT_BINARY_DIR}/update-config.cmake"
  "\
  get_filename_component(CMAKE_MODULE_PATH
    \"${CMAKE_CURRENT_SOURCE_DIR}/../cmake\" ABSOLUTE)
  set(VAST_VERSION_TAG \"${VAST_VERSION_TAG_BACKUP}\")
  set(VAST_BUILD_TYPE \"${VAST_BUILD_TYPE}\")
  set(VAST_ENABLE_ASSERTIONS \"${VAST_ENABLE_ASSERTIONS}\")
  set(VAST_ENABLE_ASAN \"${VAST_ENABLE_ASAN}\")
  set(VAST_ENABLE_UBSAN \"${VAST_ENABLE_UBSAN}\")
  include(VASTVersion)
  configure_file(\"${CMAKE_CURRENT_SOURCE_DIR}/src/config.cpp.in\"
                 \"${CMAKE_CURRENT_BINARY_DIR}/src/config.cpp\")")

# We generate config.cpp as a byproduct of a custom target so we can re-generate
# the version string and build tree hash on every build. Note that this must not
# be added to the ALL target. The dependency is inferred from the byproduct
# automatically.
add_custom_target(
  libvast_update_config
  BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/src/config.cpp"
  COMMAND ${CMAKE_COMMAND} -P "${CMAKE_CURRENT_BINARY_DIR}/update-config.cmake")

target_sources(
  libvast
  PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/include/vast/config.hpp"
          "${CMAKE_CURRENT_BINARY_DIR}/src/config.cpp"
          "${CMAKE_CURRENT_BINARY_DIR}/src/detail/installdirs.cpp")

# TODO: Should we move the bundled schemas to libvast?
if (TARGET vast-schema)
  add_dependencies(libvast vast-schema)
endif ()

# Install libvast in PREFIX/lib and headers in PREFIX/include/vast.
install(
  TARGETS libvast
  EXPORT VASTTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Runtime
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT Runtime)

# We install the static libvast_builtins archive for development targets because
# the plugin unit test binaries need to be able to link against it.
install(
  TARGETS libvast_builtins
  COMPONENT Development
  EXPORT VASTTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Runtime
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT Runtime)

install(
  DIRECTORY include/vast
  COMPONENT Development
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  FILES_MATCHING
  PATTERN "*.hpp")

# Install generated config headers.
install(
  DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/vast"
  COMPONENT Development
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  FILES_MATCHING
  PATTERN "*.hpp")

add_subdirectory(test)

set(VAST_FIND_DEPENDENCY_LIST
    "${VAST_FIND_DEPENDENCY_LIST}"
    PARENT_SCOPE)
set(VAST_EXTRA_TARGETS_FILES
    "${VAST_EXTRA_TARGETS_FILES}"
    PARENT_SCOPE)
