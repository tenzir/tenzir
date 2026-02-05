if (NOT TENZIR_ENABLE_UNIT_TESTS)
  return()
endif ()

file(GLOB_RECURSE test_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
list(SORT test_sources)

unset(suites)
foreach (test_source IN LISTS test_sources)
  get_filename_component(suite "${test_source}" NAME_WE)
  set_property(SOURCE "${test_source}" PROPERTY COMPILE_DEFINITIONS
                                                "CAF_TEST_SUITE_NAME=${suite}")
  list(APPEND suites "${suite}")
endforeach ()
list(REMOVE_DUPLICATES suites)

file(GLOB_RECURSE test_headers CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp")
list(SORT test_headers)

# Add tenzir-unit-test executable.
add_executable(tenzir-unit-test ${test_sources} ${test_headers})
TenzirTargetEnableTooling(tenzir-unit-test)
target_link_libraries(
  tenzir-unit-test PRIVATE tenzir::test tenzir::libtenzir tenzir::internal
                           ${CMAKE_THREAD_LIBS_INIT})
TenzirTargetLinkWholeArchive(tenzir-unit-test PRIVATE tenzir::libtenzir_builtins)

add_test(NAME build-tenzir-unit-test
         COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --config
                 "$<CONFIG>" --target tenzir-unit-test)
set_tests_properties(
  build-tenzir-unit-test PROPERTIES FIXTURES_SETUP tenzir_unit_test_fixture
                                    LABELS unit)

# Enable unit testing via CMake/CTest and add all test suites.
foreach (suite IN LISTS suites)
  string(REPLACE " " "_" test_name ${suite})
  add_test(NAME "libtenzir/${test_name}"
           COMMAND tenzir-unit-test -v 4 -r "${TENZIR_UNIT_TEST_TIMEOUT}" -s
                   "^${suite}$" ${ARGN})
  set_tests_properties(
    "libtenzir/${test_name}" PROPERTIES FIXTURES_REQUIRED
                                        tenzir_unit_test_fixture LABELS unit)
endforeach ()
