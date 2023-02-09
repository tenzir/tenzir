if (NOT VAST_ENABLE_UNIT_TESTS)
  return()
endif ()

file(GLOB_RECURSE test_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
list(SORT test_sources)

unset(suites)
foreach (test_source IN LISTS test_sources)
  get_filename_component(suite "${test_source}" NAME_WE)
  set_property(SOURCE "${test_source}" PROPERTY COMPILE_DEFINITIONS
                                                "SUITE=${suite}")
  list(APPEND suites "${suite}")
endforeach ()
list(REMOVE_DUPLICATES suites)

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

# Enable unit testing via CMake/CTest and add all test suites.
foreach (suite IN LISTS suites)
  string(REPLACE " " "_" test_name ${suite})
  add_test(NAME "libvast/${test_name}"
           COMMAND vast-test -v 4 -r "${VAST_UNIT_TEST_TIMEOUT}" -s
                   "^${suite}$" ${ARGN})
  set_tests_properties("libvast/${test_name}" PROPERTIES FIXTURES_REQUIRED
                                                         vast_unit_test_fixture)
endforeach ()
