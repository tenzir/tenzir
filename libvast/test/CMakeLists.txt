#
# Generate header file that defines the paths to data.
#

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

configure_file(data.hpp.in ${CMAKE_CURRENT_BINARY_DIR}/data.hpp)

#
# Configure framework
#

include_directories(
  ${CMAKE_SOURCE_DIR}/libvast
  ${CMAKE_BINARY_DIR}/libvast
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR})

set(tests
  tests/actor/export.cpp
  tests/actor/import.cpp
  tests/actor/index.cpp
  tests/actor/indexer.cpp
  tests/actor/io.cpp
  tests/actor/key_value_store.cpp
  tests/actor/partition.cpp
  tests/actor/source_bgpdump.cpp
  tests/actor/task.cpp
  tests/binner.cpp
  tests/bitmap.cpp
  tests/bitmap_index.cpp
  tests/bitstream.cpp
  tests/bitvector.cpp
  tests/block.cpp
  tests/cache.cpp
  tests/caf.cpp
  tests/chunk.cpp
  tests/data.cpp
  tests/event.cpp
  tests/expr.cpp
  tests/filesystem.cpp
  tests/getline.cpp
  tests/hash.cpp
  tests/intrusive.cpp
  tests/io.cpp
  tests/iterator.cpp
  tests/json.cpp
  tests/logging.cpp
  tests/offset.cpp
  tests/parse_bro.cpp
  tests/parse_data.cpp
  tests/parse_expr.cpp
  tests/parse_type.cpp
  tests/parse_vast.cpp
  tests/parseable.cpp
  tests/print_vast.cpp
  tests/printable.cpp
  tests/range_map.cpp
  tests/schema.cpp
  tests/search.cpp
  tests/serialization.cpp
  tests/stack.cpp
  tests/string.cpp
  tests/type.cpp
  tests/util.cpp
  tests/uuid.cpp
  tests/value.cpp
  tests/variant.cpp)

if (PCAP_FOUND)
  set(tests ${tests}
    tests/actor/source_pcap.cpp)
endif ()

add_executable(vast-test main.cpp ${tests})
target_link_libraries(vast-test libvast ${CMAKE_THREAD_LIBS_INIT})

#
# Generate CMake tests
#

enable_testing()

macro (make_test suite)
  string(REPLACE " " "_" test_name ${suite})
  set(vast_test ${EXECUTABLE_OUTPUT_PATH}/vast-test)
  add_test(${test_name} ${vast_test} -v 3 -s "${suite}" ${ARGN})
endmacro ()

foreach(test ${tests})
  file(STRINGS ${test} contents)
  foreach(line ${contents})
    if ("${line}" MATCHES "SUITE")
      string(REGEX REPLACE "#define SUITE \(.*\)" "\\1" suite ${line})
      list(APPEND suites ${suite})
    endif()
  endforeach()
endforeach()
list(REMOVE_DUPLICATES suites)

foreach(suite ${suites})
  make_test("${suite}")
endforeach ()
