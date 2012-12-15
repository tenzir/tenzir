include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/src
    ${CMAKE_BINARY_DIR}/test/unit)

# Generate C++ header file that defines the schema test cases.
file(GLOB schemas schemas/*.sch)
set (schema_test_cases "")
foreach (s ${schemas})
    get_filename_component(base ${s} NAME_WE)
    set (this "DEFINE_SCHEMA_TEST_CASE(${base}, \"${s}\")")
    set (schema_test_cases "${schema_test_cases}${this}\n")
endforeach ()
configure_file(schema_test_cases.h.in schema_test_cases.h)

file(GLOB_RECURSE tests *.cc)

# FIXME: This hack fixes a problem when the linker search path is the same as
# the one provided by the compiler. In this case, CMake replaces the absolute
# path (e.g., /path/to/lib.so) with -l<lib>, which may cause it to pick up the wrong
# library. So when this happens, we ensure that the right library gets picked
# by adding a -L directive for the affected libraries.
# We should remove this fix after having gotten rid of # Boost.Test.
set(CMAKE_EXE_LINKER_FLAGS -L${Boost_LIBRARY_DIRS})
add_executable(unit-test ${tests})
target_link_libraries(unit-test libvast ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY})

add_custom_target(test ${CMAKE_CURRENT_BINARY_DIR}/unit-test -v 0)
add_dependencies(test unit-test)
