include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/src
    ${CMAKE_BINARY_DIR}/test/unit)

# Generate C++ header file that defines the taxonomy test cases based on the
# files in the taxonomy directory.
file(GLOB taxonomies meta/taxonomies/*.tax)
set (taxonomy_test_cases "")
foreach (t ${taxonomies})
    get_filename_component(base ${t} NAME_WE)
    set (this "DEFINE_TAXONOMY_TEST_CASE(${base}, \"${t}\", \"${t}.test\")")
    set (taxonomy_test_cases "${taxonomy_test_cases}${this}\n")
endforeach ()
configure_file(meta/taxonomy_test_cases.h.in meta/taxonomy_test_cases.h)

file(GLOB_RECURSE tests *.cc)

add_executable(unit-test ${tests} ${dependencies})
target_link_libraries(unit-test vast)
