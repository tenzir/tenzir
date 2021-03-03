cmake_minimum_required(VERSION 3.14...3.19 FATAL_ERROR)

if (NOT VAST_VERSION_TAG)
  if (DEFINED ENV{VAST_VERSION_TAG})
    set(VAST_VERSION_TAG "${ENV{VAST_VERSION_TAG}}")
  elseif (EXISTS "${CMAKE_SOURCE_DIR}/.git")
    find_package(Git)
    if (Git_FOUND)
      execute_process(
        COMMAND "${GIT_EXECUTABLE}" describe --tags --long --dirty --abbrev=10
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
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
string(REGEX REPLACE "^[0-9]+\\.[0-9]+\\.[0-9]+\\-([0-9]+).*" "\\1"
                     VAST_VERSION_TWEAK "${VAST_VERSION_TAG}")
string(REGEX REPLACE "^[0-9]+\\.[0-9]+\\.[0-9]+\\-[0-9]+\\-(.*)" "\\1"
                     VAST_VERSION_COMMIT "${VAST_VERSION_TAG}")
