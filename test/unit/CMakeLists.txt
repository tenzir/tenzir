#
# Generate header file that defines the schema test cases.
#

set (schema_test_cases "")
file(GLOB schemas schemas/*.sch)
foreach (s ${schemas})
    get_filename_component(base ${s} NAME_WE)
    set (this "DEFINE_SCHEMA_TEST_CASE(${base}, \"${s}\")")
    set (schema_test_cases "${schema_test_cases}${this}\n")
endforeach ()

configure_file(tests/schema_test_cases.h.in schema_test_cases.h)

#
# Generate header file that defines the paths to log files.
#

file(GLOB_RECURSE zipped_logs bro/logs/*.gz)
if (zipped_logs)
  message(STATUS "uncompressing logs...")
  execute_process(COMMAND gunzip -fk ${zipped_logs})
endif ()

set (bro_log_header "#ifndef BRO_LOGS\n#define BRO_LOGS")
file(GLOB log_dirs bro/logs/*)
foreach (log_dir ${log_dirs})
  get_filename_component(log_dir_basename ${log_dir} NAME_WE)
  set (bro_log_header "${bro_log_header}\n\nnamespace ${log_dir_basename} {\n")
  file(GLOB logs ${log_dir}/*.log)
  foreach (log ${logs})
      get_filename_component(log_basename ${log} NAME_WE)
      set (this "constexpr auto ${log_basename} = \"${log}\";")
      set (bro_log_header "${bro_log_header}\n${this}")
  endforeach ()
  set (bro_log_header "${bro_log_header}\n\n} // namespace ${log_dir_basename}")
endforeach ()
set (bro_log_header "${bro_log_header}\n\n#endif")

configure_file(tests/bro_logs.h.in bro_logs.h)

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR})

file(GLOB_RECURSE tests framework/*.cc tests/*.cc)
add_executable(unit-test ${tests})
target_link_libraries(unit-test libvast)

#
# Generate CMake tests
#

enable_testing()

MACRO (make_test suite)
  string(REPLACE " " "_" test_name ${suite})
  set(unit_test ${EXECUTABLE_OUTPUT_PATH}/unit-test)
  add_test(${test_name} ${unit_test} -n -v 3 -s "${suite}" ${ARGN})
ENDMACRO ()

execute_process(
  COMMAND
    ${CMAKE_CURRENT_SOURCE_DIR}/find-suites ${CMAKE_CURRENT_SOURCE_DIR}/tests
  OUTPUT_VARIABLE
    test_suites)

foreach(suite ${test_suites})
  make_test("${suite}")
endforeach ()
