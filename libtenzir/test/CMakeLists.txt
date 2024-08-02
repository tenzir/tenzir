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
                                                "SUITE=${suite}")
  list(APPEND suites "${suite}")
endforeach ()
list(REMOVE_DUPLICATES suites)

file(GLOB_RECURSE test_headers CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/*.hpp")
list(SORT test_headers)

# Add tenzir-test executable.
add_executable(tenzir-test ${test_sources} ${test_headers})
TenzirTargetEnableTooling(tenzir-test)
target_link_libraries(tenzir-test PRIVATE tenzir::test tenzir::libtenzir tenzir::internal
                                        ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(tenzir-test PRIVATE tenzir::libtenzir_builtins)

add_test(NAME build-tenzir-test
         COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --config
                 "$<CONFIG>" --target tenzir-test)
set_tests_properties(build-tenzir-test PROPERTIES FIXTURES_SETUP
                                                tenzir_unit_test_fixture)

# Enable unit testing via CMake/CTest and add all test suites.
foreach (suite IN LISTS suites)
  string(REPLACE " " "_" test_name ${suite})
  add_test(NAME "libtenzir/${test_name}"
           COMMAND tenzir-test -v 4 -r "${TENZIR_UNIT_TEST_TIMEOUT}" -s
                   "^${suite}$" ${ARGN})
  set_tests_properties("libtenzir/${test_name}" PROPERTIES FIXTURES_REQUIRED
                                                         tenzir_unit_test_fixture)
endforeach ()
