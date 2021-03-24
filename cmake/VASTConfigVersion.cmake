cmake_minimum_required(VERSION 3.14...3.19 FATAL_ERROR)

set(VAST_VERSION_FALLBACK "2021.03.25-rc3-0-")

if (NOT VAST_VERSION_TAG)
  if (DEFINED ENV{VAST_VERSION_TAG})
    set(VAST_VERSION_TAG "${ENV{VAST_VERSION_TAG}}")
  elseif (EXISTS "${CMAKE_CURRENT_LIST_DIR}/../.git")
    find_package(Git QUIET)
    if (Git_FOUND)
      execute_process(
        COMMAND "${GIT_EXECUTABLE}" describe --tags --long --dirty --abbrev=10
        WORKING_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/.."
        OUTPUT_VARIABLE VAST_VERSION_TAG
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE VAST_GIT_DESCRIBE_RESULT)
      if (NOT VAST_GIT_DESCRIBE_RESULT EQUAL 0)
        message(FATAL_ERROR "git describe failed: ${VAST_GIT_DESCRIBE_RESULT}")
      endif ()
    endif ()
  else ()
    set(VAST_VERSION_TAG "${VAST_VERSION_FALLBACK}")
  endif ()
endif ()

string(REGEX REPLACE "^([0-9]+)\\..*" "\\1" VAST_VERSION_MAJOR
                     "${VAST_VERSION_TAG}")
string(REGEX REPLACE "^[0-9]+\\.([0-9]+).*" "\\1" VAST_VERSION_MINOR
                     "${VAST_VERSION_TAG}")
string(REGEX REPLACE "^[0-9]+\\.[0-9]+\\.([0-9]+).*" "\\1" VAST_VERSION_PATCH
                     "${VAST_VERSION_TAG}")
string(
  REGEX
  REPLACE "^[0-9]+\\.[0-9]+\\.[0-9]+(\\-[a-zA-Z]+[a-zA-Z0-9]*)?\\-([0-9]+).*"
          "\\2" VAST_VERSION_TWEAK "${VAST_VERSION_TAG}")

string(
  REGEX
  REPLACE "^[0-9]+\\.[0-9]+\\.[0-9]+\\-([a-zA-Z]+[a-zA-Z0-9]*)\\-[0-9]+\\-(.*)"
          "\\1-\\2" VAST_VERSION_COMMIT "${VAST_VERSION_TAG}")
if (VAST_VERSION_COMMIT STREQUAL VAST_VERSION_TAG)
  string(REGEX REPLACE "^[0-9]+\\.[0-9]+\\.[0-9]+\\-[0-9]+\\-(.*)" "\\1"
                       VAST_VERSION_COMMIT "${VAST_VERSION_TAG}")
endif ()

# Strip - and -0- suffixes from the version tag.
string(REGEX REPLACE "^(.*)[-0-|-]\\\$" "\\1" VAST_VERSION_TAG
                     "${VAST_VERSION_TAG}")

file(
  GLOB_RECURSE
  hash_files
  "${CMAKE_CURRENT_LIST_DIR}/../libvast/*.hpp"
  "${CMAKE_CURRENT_LIST_DIR}/../libvast/*.hpp.in"
  "${CMAKE_CURRENT_LIST_DIR}/../libvast/*.cpp"
  "${CMAKE_CURRENT_LIST_DIR}/../cmake/*"
  "${CMAKE_CURRENT_LIST_DIR}/../CMakeLists.txt")

list(FILTER hash_files EXCLUDE REGEX
     "^${CMAKE_CURRENT_SOURCE_DIR}/libvast/aux/")

list(FILTER hash_files EXCLUDE REGEX
     "^${CMAKE_CURRENT_SOURCE_DIR}/libvast/test/")

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

string(MD5 VAST_BUILD_TREE_HASH "${hashes}")
