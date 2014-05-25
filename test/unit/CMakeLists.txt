include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR})

# Generate C++ header file that defines the schema test cases.
file(GLOB schemas schemas/*.sch)
set (schema_test_cases "")
foreach (s ${schemas})
    get_filename_component(base ${s} NAME_WE)
    set (this "DEFINE_SCHEMA_TEST_CASE(${base}, \"${s}\")")
    set (schema_test_cases "${schema_test_cases}${this}\n")
endforeach ()
configure_file(tests/schema_test_cases.h.in schema_test_cases.h)

file(GLOB_RECURSE tests framework/*.cc tests/*.cc)
add_executable(unit-test ${tests})
target_link_libraries(unit-test libvast)

add_custom_target(
  test ${EXECUTABLE_OUTPUT_PATH}/unit-test -v 2
  DEPENDS libvast unit-test)

add_custom_target(
  test-show ${EXECUTABLE_OUTPUT_PATH}/unit-test -v 3
  DEPENDS libvast unit-test)
