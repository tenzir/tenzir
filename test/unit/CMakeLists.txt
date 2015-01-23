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
# Generate header file that defines the paths to data.
#

file(GLOB_RECURSE compressed logs/bro/*.gz logs/bgpdump/*.gz traces/*.gz)
foreach(gzip ${compressed})
  string(REGEX REPLACE "(.*)\\..+" "\\1" extracted ${gzip})
  execute_process(COMMAND gunzip -c ${gzip} OUTPUT_FILE ${extracted})
endforeach()

set (test_data "#ifndef TEST_DATA\n#define TEST_DATA")
file(GLOB log_dirs logs/bro/*)
foreach (log_dir ${log_dirs})
  get_filename_component(log_dir_basename ${log_dir} NAME_WE)
  set(test_data "${test_data}\n\nnamespace ${log_dir_basename} {\n")
  file(GLOB logs ${log_dir}/*.log)
  foreach (log ${logs})
      get_filename_component(log_basename ${log} NAME_WE)
      set (this "constexpr auto ${log_basename} = \"${log}\";")
      set (test_data "${test_data}\n${this}")
  endforeach ()
  set(test_data "${test_data}\n\n} // namespace ${log_dir_basename}")
endforeach ()

set(test_data "${test_data}\n\nnamespace bgpdump {\n")
file(GLOB logs logs/bgpdump/*.txt)
foreach (log ${logs})
  get_filename_component(log_basename ${log} NAME_WE)
  set (this "constexpr auto ${log_basename} = \"${log}\";")
  set (test_data "${test_data}\n${this}")
endforeach ()
set(test_data "${test_data}\n\n} // namespace bgpdump")

set(test_data "${test_data}\n\nnamespace traces {\n")
file(GLOB traces traces/*.pcap)
foreach (trace ${traces})
  get_filename_component(trace_basename ${trace} NAME_WE)
  set(this "constexpr auto ${trace_basename} = \"${trace}\";")
  set(test_data "${test_data}\n${this}")
endforeach ()
set(test_data "${test_data}\n\n} // namespace traces")
set(test_data "${test_data}\n\n#endif")

configure_file(tests/test_data.h.in ${CMAKE_CURRENT_BINARY_DIR}/test_data.h)

#
# Configure framework
#

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR})

file(GLOB_RECURSE tests framework/*.cc)
set(tests ${tests}
  tests/actor/distributed.cc
  tests/actor/import.cc
  tests/actor/load_balancer.cc
  tests/actor/program.cc
  tests/actor/replicator.cc
  tests/actor/source_bgpdump.cc
  tests/actor/task_tree.cc
  tests/bitmap.cc
  tests/bitmap_index.cc
  tests/bitstream.cc
  tests/bitvector.cc
  tests/block.cc
  tests/cache.cc
  tests/caf.cc
  tests/chunk.cc
  tests/data.cc
  tests/event.cc
  tests/expr.cc
  tests/factory.cc
  tests/filesystem.cc
  tests/getline.cc
  tests/hash.cc
  tests/intrusive.cc
  tests/io.cc
  tests/iterator.cc
  tests/json.cc
  tests/logging.cc
  tests/offset.cc
  tests/parse.cc
  tests/print.cc
  tests/range_map.cc
  tests/schema.cc
  tests/search.cc
  tests/serialization.cc
  tests/stack_alloc.cc
  tests/string.cc
  tests/type.cc
  tests/util.cc
  tests/uuid.cc
  tests/value.cc
  tests/variant.cc)

if (PCAP_FOUND)
  set(tests ${tests}
    tests/actor/source_pcap.cc)
endif ()


add_executable(unit-test ${tests})
target_link_libraries(unit-test libvast ${CMAKE_THREAD_LIBS_INIT})

#
# Generate CMake tests
#

enable_testing()

MACRO (make_test suite)
  string(REPLACE " " "_" test_name ${suite})
  set(unit_test ${EXECUTABLE_OUTPUT_PATH}/unit-test)
  add_test(${test_name} ${unit_test} -n -v 3 -s "${suite}" ${ARGN})
ENDMACRO ()

foreach(test ${tests})
  file(STRINGS ${test} contents)
  foreach(line ${contents})
    if ("${line}" MATCHES "SUITE(.*)")
      string(REGEX REPLACE ".*\\(\"(.*)\"\\).*" "\\1" suite ${line})
      list(APPEND suites ${suite})
    endif()
  endforeach()
endforeach()
list(REMOVE_DUPLICATES suites)


foreach(suite ${suites})
  make_test("${suite}")
endforeach ()
