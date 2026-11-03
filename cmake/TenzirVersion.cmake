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

# The tag of the sources the build has, which deployment images are tagged with
# and a deployment reports, so the platform can tell which of the builds in its
# history it runs. A Nix build passes it, since its sources are no checkout. In
# the monorepo, it is computed from the tracked files when configuring and on
# every build; without the sibling deployment project, there are no such images
# and no tag.
if (NOT DEFINED TENZIR_SOURCE_TAG)
  set(TENZIR_SOURCE_TAG "")
  if (Git_FOUND AND EXISTS
                    "${CMAKE_CURRENT_LIST_DIR}/../../deployment/CMakeLists.txt")
    execute_process(
      COMMAND sh scripts/source-tag.sh files . ../deployment
      WORKING_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/.."
      OUTPUT_VARIABLE _source_tag_output
      OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE _source_tag_result)
    if (_source_tag_result EQUAL 0)
      set(TENZIR_SOURCE_TAG "${_source_tag_output}")
    else ()
      message(WARNING "source-tag.sh failed: ${_source_tag_result}; "
                      "the build reports no source tag")
    endif ()
    unset(_source_tag_output)
    unset(_source_tag_result)
  endif ()
endif ()

string(REGEX REPLACE "([^-]+)-.*" "\\1" TENZIR_VERSION_MMP "${TENZIR_VERSION}")
set(TENZIR_VERSION_FULL "${TENZIR_VERSION}")
if (TENZIR_VERSION_SUFFIX)
  string(APPEND TENZIR_VERSION_FULL "+${TENZIR_VERSION_SUFFIX}")
endif ()
