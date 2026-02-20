# Build folly, fizz, wangle, mvfst, and proxygen from submodules as static
# archives. All five libraries are linked into libtenzir.so so that there is
# exactly one copy of every folly singleton in the process.

# Validate all submodules exist. Some repos (fizz, wangle) have their
# CMakeLists.txt in a subdirectory named after the project.
foreach (_fb_lib folly mvfst proxygen)
  if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${_fb_lib}/CMakeLists.txt")
    message(
      FATAL_ERROR
        "${_fb_lib} not found. Initialize submodule libtenzir/aux/facebook/${_fb_lib}"
    )
  endif ()
endforeach ()
foreach (_fb_lib fizz wangle)
  if (NOT
      EXISTS
      "${CMAKE_CURRENT_SOURCE_DIR}/${_fb_lib}/${_fb_lib}/CMakeLists.txt")
    message(
      FATAL_ERROR
        "${_fb_lib} not found. Initialize submodule libtenzir/aux/facebook/${_fb_lib}"
    )
  endif ()
endforeach ()

# Common setup
find_package(glog REQUIRED)

# Prefer bundled Facebook headers over any globally installed copies (e.g.
# Homebrew) so all five libs compile against a consistent API surface.
include_directories(
  BEFORE
  "${CMAKE_CURRENT_SOURCE_DIR}/folly"
  "${CMAKE_CURRENT_SOURCE_DIR}/fizz"
  "${CMAKE_CURRENT_SOURCE_DIR}/wangle"
  "${CMAKE_CURRENT_SOURCE_DIR}/mvfst"
  "${CMAKE_CURRENT_SOURCE_DIR}/proxygen")

# Configure folly build options
set(BUILD_TESTS
    OFF
    CACHE BOOL "" FORCE)
set(BUILD_BENCHMARKS
    OFF
    CACHE BOOL "")
set(BUILD_EXAMPLES
    OFF
    CACHE BOOL "" FORCE)
# gflags is available from the Nix environment (transitive dep of glog).
# With all Facebook libs as static archives inside libtenzir.so, gflags
# registrations go into the single shared libgflags — no duplication.
# Disable malloc_usable_size - conflicts with jemalloc's symbol renaming
set(FOLLY_HAVE_MALLOC_USABLE_SIZE
    OFF
    CACHE BOOL "")
# Point folly to our bundled fast_float.
if (TENZIR_ENABLE_BUNDLED_FASTFLOAT)
  set(FASTFLOAT_INCLUDE_DIR
      "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/libtenzir/aux/fast_float/include>"
      CACHE PATH "")
endif ()
# Find blake3 if available - folly uses __has_include(<blake3.h>) to compile
# blake3 support, so we must link against it when the header is found.
find_package(blake3 CONFIG QUIET)
if (blake3_FOUND)
  list(APPEND FOLLY_LINK_LIBRARIES BLAKE3::blake3)
endif ()

# Pass SSE/AVX flags to all Facebook libs to ensure consistent SIMD settings
# across all compilation units. Without this, F14Table.cpp may be compiled
# with different SIMD settings than code using F14 headers, causing link
# errors due to F14LinkCheck mismatch.
if (TENZIR_ENABLE_SSE_INSTRUCTIONS)
  string(APPEND CMAKE_CXX_FLAGS " -msse")
endif ()
if (TENZIR_ENABLE_SSE2_INSTRUCTIONS)
  string(APPEND CMAKE_CXX_FLAGS " -msse2")
endif ()
if (TENZIR_ENABLE_SSE3_INSTRUCTIONS)
  string(APPEND CMAKE_CXX_FLAGS " -msse3")
endif ()
if (TENZIR_ENABLE_SSSE3_INSTRUCTIONS)
  string(APPEND CMAKE_CXX_FLAGS " -mssse3")
endif ()
if (TENZIR_ENABLE_SSE4_1_INSTRUCTIONS)
  string(APPEND CMAKE_CXX_FLAGS " -msse4.1")
endif ()
if (TENZIR_ENABLE_SSE4_2_INSTRUCTIONS)
  string(APPEND CMAKE_CXX_FLAGS " -msse4.2")
endif ()
if (TENZIR_ENABLE_AVX_INSTRUCTIONS)
  string(APPEND CMAKE_CXX_FLAGS " -mavx")
endif ()
if (TENZIR_ENABLE_AVX2_INSTRUCTIONS)
  string(APPEND CMAKE_CXX_FLAGS " -mavx2")
endif ()

# Force ALL Facebook libs to build as static libraries. This ensures every
# folly singleton lives in exactly one place (libtenzir.so), preventing the
# "mismatched unique instance" crash from duplicate folly singletons.
# When the main build uses shared libraries, static archives must be compiled
# with -fPIC because they will be linked into a shared library.
if (BUILD_SHARED_LIBS)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif ()
set(BUILD_SHARED_LIBS OFF)

# Skip install rules while adding bundled Facebook libs — they are static
# archives linked into libtenzir.so and never installed individually.
# Restore the previous value afterwards so this directory still gets a
# cmake_install.cmake for parent include() calls.
set(_tenzir_prev_skip_install_rules "${CMAKE_SKIP_INSTALL_RULES}")
set(CMAKE_SKIP_INSTALL_RULES ON)

# ---- 1. FOLLY ----
add_subdirectory(folly SYSTEM)
if (NOT TARGET Folly::folly)
  add_library(Folly::folly ALIAS folly)
endif ()
# Write shim config files so downstream find_package() calls are no-ops
# when the target is already defined from add_subdirectory(). The generated
# folly-config.cmake has set_and_check() calls that fail because the build
# tree doesn't match folly's expected install layout.
set(_fb_shim_dir "${CMAKE_BINARY_DIR}/_fb_shims")
file(MAKE_DIRECTORY "${_fb_shim_dir}/folly" "${_fb_shim_dir}/fizz"
     "${_fb_shim_dir}/wangle" "${_fb_shim_dir}/mvfst")
file(
  WRITE "${_fb_shim_dir}/folly/folly-config.cmake"
  "# Shim: target already defined via add_subdirectory()\n"
  "if(NOT TARGET Folly::folly)\n"
  "  message(FATAL_ERROR \"Folly::folly target not found\")\n"
  "endif()\n"
  "set(FOLLY_LIBRARIES Folly::folly)\n")
set(folly_DIR
    "${_fb_shim_dir}/folly"
    CACHE PATH "" FORCE)
dependency_summary("folly" "${CMAKE_CURRENT_SOURCE_DIR}/folly" "Dependencies")

# ---- 2. FIZZ ----
# fizz's CMakeLists.txt is in fizz/fizz/ (repo root has no CMakeLists.txt)
add_subdirectory(fizz/fizz SYSTEM)
# Fix include path: fizz uses CMAKE_SOURCE_DIR which is wrong for subdirectory
# builds
target_include_directories(
  fizz BEFORE
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/fizz>)
if (NOT TARGET fizz::fizz)
  add_library(fizz::fizz ALIAS fizz)
endif ()
file(
  WRITE "${_fb_shim_dir}/fizz/fizz-config.cmake"
  "# Shim: target already defined via add_subdirectory()\n"
  "if(NOT TARGET fizz::fizz)\n"
  "  message(FATAL_ERROR \"fizz::fizz target not found\")\n"
  "endif()\n"
  "set(FIZZ_LIBRARIES fizz::fizz)\n")
set(fizz_DIR
    "${_fb_shim_dir}/fizz"
    CACHE PATH "" FORCE)
set(Fizz_DIR
    "${_fb_shim_dir}/fizz"
    CACHE PATH "" FORCE)

# ---- 3. WANGLE ----
# wangle's CMakeLists.txt is in wangle/wangle/
add_subdirectory(wangle/wangle SYSTEM)
# Fix include path (same CMAKE_SOURCE_DIR issue)
target_include_directories(
  wangle BEFORE
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/wangle>)
if (NOT TARGET wangle::wangle)
  add_library(wangle::wangle ALIAS wangle)
endif ()
file(
  WRITE "${_fb_shim_dir}/wangle/wangle-config.cmake"
  "# Shim: target already defined via add_subdirectory()\n"
  "if(NOT TARGET wangle::wangle)\n"
  "  message(FATAL_ERROR \"wangle::wangle target not found\")\n"
  "endif()\n"
  "set(WANGLE_LIBRARIES wangle::wangle)\n")
set(wangle_DIR
    "${_fb_shim_dir}/wangle"
    CACHE PATH "" FORCE)

# ---- 4. MVFST ----
# mvfst's CMakeLists.txt is at repo root
add_subdirectory(mvfst SYSTEM)
# mvfst's install(EXPORT) adds the mvfst:: namespace, but add_subdirectory
# only creates bare targets. Create aliases for all targets that proxygen
# and other downstream libs reference with the mvfst:: prefix.
file(
  GLOB_RECURSE
  _mvfst_cmake_files
  "${CMAKE_CURRENT_SOURCE_DIR}/mvfst/quic/*/CMakeLists.txt")
foreach (_mvfst_cmake_file IN LISTS _mvfst_cmake_files)
  file(READ "${_mvfst_cmake_file}" _mvfst_cmake_content)
  string(REGEX MATCHALL "TARGETS mvfst_[a-z_]+" _mvfst_matches
               "${_mvfst_cmake_content}")
  foreach (_mvfst_match IN LISTS _mvfst_matches)
    string(REGEX REPLACE "^TARGETS " "" _mvfst_tgt "${_mvfst_match}")
    if (TARGET ${_mvfst_tgt} AND NOT TARGET mvfst::${_mvfst_tgt})
      add_library(mvfst::${_mvfst_tgt} ALIAS ${_mvfst_tgt})
    endif ()
  endforeach ()
endforeach ()
file(WRITE "${_fb_shim_dir}/mvfst/mvfst-config.cmake"
     "# Shim: target already defined via add_subdirectory()\n")
set(mvfst_DIR
    "${_fb_shim_dir}/mvfst"
    CACHE PATH "" FORCE)

# ---- 5. PROXYGEN ----
add_subdirectory(proxygen SYSTEM)
# Newer glog (0.7+) no longer transitively includes <gflags/gflags.h> in
# <glog/logging.h>. Proxygen source files use raw DEFINE_int32/DEFINE_bool
# macros without explicitly including gflags. Force-include the header.
target_compile_options(proxygen PRIVATE -include gflags/gflags.h)
TenzirSystemizeTarget(proxygenhttpserver)
if (NOT TARGET proxygen::proxygenhttpserver)
  add_library(proxygen::proxygenhttpserver ALIAS proxygenhttpserver)
endif ()
dependency_summary("proxygen" "${CMAKE_CURRENT_SOURCE_DIR}/proxygen"
                   "Dependencies")

# CMAKE_SKIP_INSTALL_RULES suppresses generation of per-directory
# cmake_install.cmake files. The parent facebook install script still
# includes these paths, so create no-op stubs to keep install steps working.
foreach (_tenzir_fb_install_subdir folly fizz/fizz wangle/wangle mvfst proxygen)
  set(
    _tenzir_fb_install_stub
    "${CMAKE_CURRENT_BINARY_DIR}/${_tenzir_fb_install_subdir}/cmake_install.cmake"
  )
  get_filename_component(_tenzir_fb_install_stub_dir
                         "${_tenzir_fb_install_stub}" DIRECTORY)
  file(MAKE_DIRECTORY "${_tenzir_fb_install_stub_dir}")
  if (NOT EXISTS "${_tenzir_fb_install_stub}")
    file(
      WRITE "${_tenzir_fb_install_stub}"
      "# Bundled via Tenzir; install rules intentionally skipped.\n")
  endif ()
endforeach ()
unset(_tenzir_fb_install_subdir)
unset(_tenzir_fb_install_stub)
unset(_tenzir_fb_install_stub_dir)

# All Facebook libs are static archives linked into libtenzir.so. No
# export/install of individual targets needed.
set(CMAKE_SKIP_INSTALL_RULES "${_tenzir_prev_skip_install_rules}")
unset(_tenzir_prev_skip_install_rules)
set(_tenzir_bundled_facebook
    ""
    PARENT_SCOPE)
