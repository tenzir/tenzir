file(READ "${CMAKE_CURRENT_LIST_DIR}/../version.json" TENZIR_VERSION_JSON)
string(JSON TENZIR_VERSION GET "${TENZIR_VERSION_JSON}" tenzir-version)
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
      string(REGEX MATCH "(g[0-9a-f]+)?(-dirty)?$" _git_describe_suffix
                   "${_git_describe_output}")
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

string(REGEX REPLACE "([^-]+)-.*" "\\1" TENZIR_VERSION_MMP "${TENZIR_VERSION}")
set(TENZIR_VERSION_FULL "${TENZIR_VERSION}")
if (TENZIR_VERSION_BUILD_METADATA)
  string(APPEND TENZIR_VERSION_FULL "+${TENZIR_VERSION_BUILD_METADATA}")
endif ()
