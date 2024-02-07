file(READ "${CMAKE_CURRENT_LIST_DIR}/../version.json" TENZIR_VERSION_JSON)
string(JSON TENZIR_VERSION GET "${TENZIR_VERSION_JSON}"
       tenzir-version)
string(JSON TENZIR_PARTITION_VERSION GET "${TENZIR_VERSION_JSON}"
       tenzir-partition-version)

find_package(Git QUIET)

if (NOT DEFINED TENZIR_VERSION_BUILD_METADATA)
  if (Git_FOUND)
    execute_process(
      COMMAND "${GIT_EXECUTABLE}" describe --abbrev=10 --long --dirty
              "--match=v[0-9]*"
      WORKING_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/.."
      OUTPUT_VARIABLE _git_describe_output
      OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE _git_describe_result)
    if (NOT _git_describe_result EQUAL 0)
      message(
        WARNING
          "git-describe failed: ${_git_describe_result}; using a generic \"-dev\" version suffix"
      )
    else ()
      string(REGEX MATCH "(g[0-9a-f]+)?(-dirty)?$" _git_describe_suffix "${_git_describe_output}")
      if (_git_describe_suffix)
        set(TENZIR_VERSION_BUILD_METADATA "${_git_describe_suffix}")
      endif ()
    endif ()
    unset(_git_describe_output)
    unset(_git_describe_result)
  endif ()
endif ()

if (NOT DEFINED TENZIR_VERSION_BUILD_METADATA)
  set(TENZIR_VERSION_BUILD_METADATA "unknown")
endif ()

set(TENZIR_VERSION_FULL "${TENZIR_VERSION}+${TENZIR_VERSION_BUILD_METADATA}")

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
  "${parent_dir}/libtenzir/*.cpp"
  "${parent_dir}/libtenzir/*.cpp.in"
  "${parent_dir}/libtenzir/*.fbs"
  "${parent_dir}/libtenzir/*.hpp"
  "${parent_dir}/libtenzir/*.hpp.in"
  "${parent_dir}/cmake/*"
  "${parent_dir}/CMakeLists.txt")
list(SORT hash_files)

cmake_policy(POP)

list(FILTER hash_files EXCLUDE REGEX "^${parent_dir}/libtenzir/aux/")
list(FILTER hash_files EXCLUDE REGEX "^${parent_dir}/libtenzir/test/")

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
