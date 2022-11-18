if (NOT VAST_ENABLE_UNIT_TESTS)
  return()
endif ()

file(GLOB_RECURSE test_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
list(SORT test_sources)

file(GLOB_RECURSE test_headers CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp")
list(SORT test_headers)

# Add vast-test executable.
add_executable(vast-test ${test_sources} ${test_headers})
VASTTargetEnableTooling(vast-test)
target_link_libraries(vast-test PRIVATE vast::test vast::libvast vast::internal
                                        ${CMAKE_THREAD_LIBS_INIT})
VASTTargetLinkWholeArchive(vast-test PRIVATE vast::libvast_builtins)

add_test(NAME build-vast-test
         COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --config
                 "$<CONFIG>" --target vast-test)
set_tests_properties(build-vast-test PROPERTIES FIXTURES_SETUP
                                                vast_unit_test_fixture)
# Helper macro to construct a CMake test from a VAST test suite.
macro (make_test suite)
  string(REPLACE " " "_" test_name ${suite})
  add_test(NAME ${test_name}
           COMMAND vast-test -v 4 -r "${VAST_UNIT_TEST_TIMEOUT}" -s
                   "^${suite}$" ${ARGN})
  set_tests_properties(${test_name} PROPERTIES FIXTURES_REQUIRED
                                               vast_unit_test_fixture)
endmacro ()

# Find all test suites.
foreach (test ${test_sources})
  file(STRINGS ${test} contents)
  foreach (line ${contents})
    if ("${line}" MATCHES "^ *# *define +SUITE +[^ ]+ *$")
      string(REGEX REPLACE "^ *# *define +SUITE +\([^ ]+\) *$" "\\1" suite
                           ${line})
      list(APPEND suites ${suite})
      # We only support a single suite per source file, so we can stop
      # considering the remaining lines in this file.
      break()
    endif ()
  endforeach ()
endforeach ()
list(REMOVE_DUPLICATES suites)
# Enable unit testing via CMake/CTest and add all test suites.
foreach (suite ${suites})
  make_test("${suite}")
endforeach ()
