file(READ "${CMAKE_CURRENT_LIST_DIR}/../version.json" TENZIR_VERSION_JSON)
string(JSON TENZIR_VERSION_FALLBACK GET "${TENZIR_VERSION_JSON}"
       tenzir-version-fallback)
string(JSON TENZIR_PARTITION_VERSION GET "${TENZIR_VERSION_JSON}"
       vast-partition-version)

find_package(Git QUIET)

if (NOT TENZIR_VERSION_TAG)
  if (DEFINED ENV{TENZIR_VERSION_TAG})
    set(TENZIR_VERSION_TAG "${ENV{TENZIR_VERSION_TAG}}")
  elseif (EXISTS "${CMAKE_CURRENT_LIST_DIR}/../.git")
    if (Git_FOUND)
      execute_process(
        COMMAND "${GIT_EXECUTABLE}" describe --abbrev=10 --long --dirty
                "--match=v[0-9]*"
        WORKING_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/.."
        OUTPUT_VARIABLE TENZIR_VERSION_TAG
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE TENZIR_GIT_DESCRIBE_RESULT)
      if (NOT TENZIR_GIT_DESCRIBE_RESULT EQUAL 0)
        message(
          WARNING
            "git-describe failed: ${TENZIR_GIT_DESCRIBE_RESULT}; using fallback version"
        )
        unset(TENZIR_VERSION_TAG)
      endif ()
    endif ()
  endif ()
endif ()

if (NOT TENZIR_VERSION_SHORT)
  if (DEFINED ENV{TENZIR_VERSION_SHORT})
    set(TENZIR_VERSION_SHORT "${ENV{TENZIR_VERSION_SHORT}}")
  elseif (EXISTS "${CMAKE_CURRENT_LIST_DIR}/../.git")
    if (Git_FOUND)
      # Emit a "non-long" version for use in package file names.
      execute_process(
        COMMAND "${GIT_EXECUTABLE}" describe --abbrev=10 "--match=v[0-9]*"
        WORKING_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/.."
        OUTPUT_VARIABLE TENZIR_VERSION_SHORT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE TENZIR_GIT_DESCRIBE_RESULT)
      if (NOT TENZIR_GIT_DESCRIBE_RESULT EQUAL 0)
        message(
          WARNING
            "git-describe failed: ${TENZIR_GIT_DESCRIBE_RESULT}; using fallback version"
        )
        unset(TENZIR_VERSION_SHORT)
      endif ()
    endif ()
  endif ()
endif ()

if (NOT TENZIR_VERSION_TAG)
  set(TENZIR_VERSION_TAG "v${TENZIR_VERSION_FALLBACK}")
endif ()

if (NOT TENZIR_VERSION_SHORT)
  set(TENZIR_VERSION_SHORT "v${TENZIR_VERSION_FALLBACK}")
endif ()

# We accept:
# (1) v1.0.0(-dirty)
# (2) v1.0.0-rc1(-dirty)
# (3) v1.0.0-13-g3c8009fe4(-dirty)
# (4) v1.0.0-rc1-13-g3c8009fe4(-dirty)

list(LENGTH "${TENZIR_VERSION_TAG}" version_tag_list_len)
if (NOT version_tag_list_len EQUAL 0)
  message(FATAL_ERROR "Invalid version tag: ${TENZIR_VERSION_TAG}")
endif ()
unset(version_tag_list_len)

# Strip the v prefix from the version tag, if it exists.
string(REGEX REPLACE "^v" "" version_list "${TENZIR_VERSION_TAG}")

string(REPLACE "-" ";" version_list "${version_list}")
# The version string can optionally have a "-dirty" suffix, we strip it now to
# get consistent reverse indexing.
list(GET version_list -1 version_list_last)
if (NOT version_list_last OR version_list_last STREQUAL "dirty")
  list(POP_BACK version_list)
endif ()
list(LENGTH version_list version_list_len)

# Extract Major, Minor, and Patch.
list(GET version_list 0 TENZIR_VERSION_MMP)
string(REPLACE "." ";" TENZIR_VERSION_MMP "${TENZIR_VERSION_MMP}")
list(GET TENZIR_VERSION_MMP 0 TENZIR_VERSION_MAJOR)
list(GET TENZIR_VERSION_MMP 1 TENZIR_VERSION_MINOR)
list(GET TENZIR_VERSION_MMP 2 TENZIR_VERSION_PATCH)

# Extract Tweak and Commit.
if (version_list_len GREATER 2)
  list(GET version_list -2 TENZIR_VERSION_TWEAK)
  list(GET version_list -1 TENZIR_VERSION_COMMIT)
else ()
  # Default tweak to 0 and commit to unset.
  set(TENZIR_VERSION_TWEAK 0)
endif ()

unset(version_list)
unset(version_list_last)
unset(version_list_len)

cmake_policy(PUSH)
if (POLICY CMP0009)
  # Do not follow symlinks in FILE GLOB_RECURSE by default.
  # https://cmake.org/cmake/help/latest/policy/CMP0009.html
  cmake_policy(SET CMP0009 NEW)
endif ()

get_filename_component(parent_dir "${CMAKE_CURRENT_LIST_DIR}" DIRECTORY)
file(
  GLOB_RECURSE
  hash_files
  "${parent_dir}/libvast/*.cpp"
  "${parent_dir}/libvast/*.cpp.in"
  "${parent_dir}/libvast/*.fbs"
  "${parent_dir}/libvast/*.hpp"
  "${parent_dir}/libvast/*.hpp.in"
  "${parent_dir}/cmake/*"
  "${parent_dir}/CMakeLists.txt")
list(SORT hash_files)

cmake_policy(POP)

list(FILTER hash_files EXCLUDE REGEX "^${parent_dir}/libvast/aux/")
list(FILTER hash_files EXCLUDE REGEX "^${parent_dir}/libvast/test/")

list(
  SORT hash_files
  COMPARE FILE_BASENAME
  CASE INSENSITIVE)

foreach (hash_file IN LISTS hash_files)
  if (EXISTS "${hash_file}" AND NOT IS_DIRECTORY "${hash_file}")
    file(MD5 "${hash_file}" hash)
    list(APPEND hashes "${hash}")
  endif ()
endforeach ()

string(MD5 TENZIR_BUILD_TREE_HASH "${hashes}")
