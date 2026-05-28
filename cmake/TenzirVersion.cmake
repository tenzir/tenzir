file(READ "${CMAKE_CURRENT_LIST_DIR}/../version.json" TENZIR_VERSION_JSON)
string(JSON TENZIR_VERSION GET "${TENZIR_VERSION_JSON}" tenzir-version)
string(JSON TENZIR_PARTITION_VERSION GET "${TENZIR_VERSION_JSON}"
       tenzir-partition-version)

find_package(Git QUIET)

if (NOT DEFINED TENZIR_VERSION_BUILD_METADATA)
  if (Git_FOUND)
    execute_process(
      COMMAND "${GIT_EXECUTABLE}" rev-parse --short=10 HEAD
      WORKING_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/.."
      OUTPUT_VARIABLE _git_revision_output
      OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE _git_revision_result)
    if (NOT _git_revision_result EQUAL 0)
      message(
        WARNING
          "git-rev-parse failed: ${_git_revision_result}; using a generic version build metadata"
      )
    else ()
      if (_git_revision_output)
        set(TENZIR_VERSION_BUILD_METADATA "g${_git_revision_output}")
      endif ()
    endif ()
    unset(_git_revision_output)
    unset(_git_revision_result)
  endif ()
endif ()

if (NOT DEFINED TENZIR_VERSION_BUILD_METADATA)
  set(TENZIR_VERSION_BUILD_METADATA "unknown")
endif ()

string(REGEX REPLACE "([^-]+)-.*" "\\1" TENZIR_VERSION_MMP "${TENZIR_VERSION}")
set(TENZIR_VERSION_FULL "${TENZIR_VERSION}")
if (TENZIR_VERSION_SUFFIX)
  string(APPEND TENZIR_VERSION_FULL "+${TENZIR_VERSION_SUFFIX}")
endif ()
