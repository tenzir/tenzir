cmake_minimum_required(VERSION 3.14...3.19 FATAL_ERROR)

# Get the version tag from 'git describe' if it is not set explicitly.
if (NOT VAST_VERSION_TAG)
  find_package(Git)
  if (Git_FOUND AND EXISTS ${CMAKE_SOURCE_DIR}/.git)
    execute_process(
      COMMAND "${GIT_EXECUTABLE}" describe --tags --long --dirty
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
      OUTPUT_VARIABLE VAST_VERSION_TAG
      OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE VAST_GIT_DESCRIBE_RESULT)
    if (NOT VAST_GIT_DESCRIBE_RESULT EQUAL 0)
      message(FATAL_ERROR "git describe failed: ${VAST_GIT_DESCRIBE_RESULT}")
    endif ()
  endif ()
endif ()

string(REGEX REPLACE "^([0-9]+)\\..*" "\\1" VAST_VERSION_MAJOR "${VAST_VERSION_TAG}")
string(REGEX REPLACE "^[0-9]+\\.([0-9]+).*" "\\1" VAST_VERSION_MINOR
  "${VAST_VERSION_TAG}")
string(REGEX REPLACE "^[0-9]+\\.[0-9]+\\.([0-9]+).*" "\\1" VAST_VERSION_PATCH
  "${VAST_VERSION_TAG}")
string(REGEX REPLACE "^[0-9]+\\.[0-9]+\\.[0-9]+\\-([0-9]+).*" "\\1"
  VAST_VERSION_TWEAK "${VAST_VERSION_TAG}")
string(REGEX REPLACE "^[0-9]+\\.[0-9]+\\.[0-9]+\\-[0-9]+\\-(.*)" "\\1"
  VAST_VERSION_COMMIT "${VAST_VERSION_TAG}")
