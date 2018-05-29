# ----------------------------------------------------------------------------
#                                   libvast
# ----------------------------------------------------------------------------

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/vast/config.hpp.in
               ${CMAKE_CURRENT_BINARY_DIR}/vast/config.hpp)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Some CMake generators (e.g., XCode) require header files in
# addition to the sources.
file(GLOB_RECURSE libvast_headers "${CMAKE_CURRENT_SOURCE_DIR}/vast/*.hpp")

set(libvast_sources
  src/address.cpp
  src/attribute.cpp
  src/banner.cpp
  src/base.cpp
  src/batch.cpp
  src/bitmap.cpp
  src/chunk.cpp
  src/command.cpp
  src/compression.cpp
  src/concept/hashable/crc.cpp
  src/concept/hashable/xxhash.cpp
  src/data.cpp
  src/detail/adjust_resource_consumption.cpp
  src/detail/compressedbuf.cpp
  src/detail/fdinbuf.cpp
  src/detail/fdistream.cpp
  src/detail/fdostream.cpp
  src/detail/fdoutbuf.cpp
  src/detail/line_range.cpp
  src/detail/make_io_stream.cpp
  src/detail/mmapbuf.cpp
  src/detail/posix.cpp
  src/detail/string.cpp
  src/detail/system.cpp
  src/detail/terminal.cpp
  src/die.cpp
  src/error.cpp
  src/event.cpp
  src/ewah_bitmap.cpp
  src/expression.cpp
  src/expression_visitors.cpp
  src/filesystem.cpp
  src/format/bgpdump.cpp
  src/format/bro.cpp
  src/format/csv.cpp
  src/format/mrt.cpp
  src/format/test.cpp
  src/http.cpp
  src/ids.cpp
  src/key.cpp
  src/null_bitmap.cpp
  src/operator.cpp
  src/option_declaration_set.cpp
  src/option_map.cpp
  src/pattern.cpp
  src/port.cpp
  src/schema.cpp
  src/segment_store.cpp
  src/subnet.cpp
  src/system/accountant.cpp
  src/system/application.cpp
  src/system/archive.cpp
  src/system/column_index.cpp
  src/system/configuration.cpp
  src/system/consensus.cpp
  src/system/default_application.cpp
  src/system/export_command.cpp
  src/system/exporter.cpp
  src/system/import_command.cpp
  src/system/importer.cpp
  src/system/index.cpp
  src/system/indexer.cpp
  src/system/indexer_manager.cpp
  src/system/indexer_stage_driver.cpp
  src/system/node.cpp
  src/system/node_command.cpp
  src/system/partition.cpp
  src/system/profiler.cpp
  src/system/reader_command_base.cpp
  src/system/remote_command.cpp
  src/system/signal_monitor.cpp
  src/system/spawn.cpp
  src/system/spawn_sink.cpp
  src/system/spawn_source.cpp
  src/system/start_command.cpp
  src/system/table_index.cpp
  src/system/task.cpp
  src/system/tracker.cpp
  src/system/writer_command_base.cpp
  src/time.cpp
  src/type.cpp
  src/uuid.cpp
  src/value.cpp
  src/value_index.cpp
  src/view.cpp
  src/wah_bitmap.cpp
)

if (PCAP_FOUND)
  set(libvast_sources ${libvast_sources}
    src/system/pcap_reader_command.cpp
    src/system/pcap_writer_command.cpp
    src/format/pcap.cpp)
endif ()

set(libvast_libs ${CAF_LIBRARIES})

if (VAST_ENABLE_ASSERTIONS)
  set(libvast_libs ${libvast_libs} ${Backtrace_LIBRARIES})
endif ()

if (SNAPPY_FOUND)
  set(libvast_libs ${libvast_libs} ${SNAPPY_LIBRARIES})
endif ()

if (PCAP_FOUND)
  set(libvast_libs ${libvast_libs} ${PCAP_LIBRARIES})
endif ()

# Always link with -lprofile if we have Gperftools.
if (GPERFTOOLS_FOUND)
  set(libvast_libs ${libvast_libs} ${GPERFTOOLS_PROFILER})
endif ()

# Only link against tcmalloc if requested.
if (GPERFTOOLS_FOUND AND VAST_USE_PERFTOOLS_HEAP_PROFILER)
  set(libvast_libs ${libvast_libs} ${GPERFTOOLS_TCMALLOC})
endif ()

add_library(libvast SHARED ${libvast_sources} ${libvast_headers})
set_target_properties(libvast
  PROPERTIES
  SOVERSION ${VERSION_MAJOR}
  VERSION ${VERSION_MAJOR}.${VERSION_MINOR}
  OUTPUT_NAME vast)

target_link_libraries(libvast ${libvast_libs})

# Install libvast in PREFIX/lib and headers in PREFIX/include/vast.
install(TARGETS libvast DESTINATION lib)
install(DIRECTORY vast/
        DESTINATION include/vast FILES_MATCHING PATTERN "*.hpp")

# Install generated config header.
install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/vast/"
        DESTINATION include/vast FILES_MATCHING PATTERN "*.hpp")

# ----------------------------------------------------------------------------
#                                 unit tests
# ----------------------------------------------------------------------------

# Bro logs.
set (test_data "#ifndef TEST_DATA\n#define TEST_DATA")
set(test_data "${test_data}\n\nnamespace bro {\n")
file(GLOB logs test/logs/bro/*)
foreach (log ${logs})
  get_filename_component(log_basename ${log} NAME_WE)
  set (this "constexpr auto ${log_basename} = \"${log}\";")
  set (test_data "${test_data}\n${this}")
endforeach ()
set(test_data "${test_data}\n\n} // namespace bro")

# Bgpdump logs.
set(test_data "${test_data}\n\nnamespace bgpdump {\n")
file(GLOB logs test/logs/bgpdump/*.txt)
foreach (log ${logs})
  get_filename_component(log_basename ${log} NAME_WE)
  set (this "constexpr auto ${log_basename} = \"${log}\";")
  set (test_data "${test_data}\n${this}")
endforeach ()
set(test_data "${test_data}\n\n} // namespace bgpdump")

# MRT logs.
set(test_data "${test_data}\n\nnamespace mrt {\n")
file(GLOB logs test/logs/mrt/*)
foreach (log ${logs})
  get_filename_component(log_basename ${log} NAME_WE)
  set (this "constexpr auto ${log_basename} = \"${log}\";")
  set (test_data "${test_data}\n${this}")
endforeach ()
set(test_data "${test_data}\n\n} // namespace mrt")

# PCAP traces.
set(test_data "${test_data}\n\nnamespace traces {\n")
file(GLOB traces test/traces/*.pcap)
foreach (trace ${traces})
  get_filename_component(trace_basename ${trace} NAME_WE)
  set(this "constexpr auto ${trace_basename} = \"${trace}\";")
  set(test_data "${test_data}\n${this}")
endforeach ()
set(test_data "${test_data}\n\n} // namespace traces")
set(test_data "${test_data}\n\n#endif")

configure_file(test/data.hpp.in ${CMAKE_CURRENT_BINARY_DIR}/test/data.hpp)

include_directories(
  ${CMAKE_SOURCE_DIR}/libvast
  ${CMAKE_BINARY_DIR}/libvast
  ${CMAKE_CURRENT_SOURCE_DIR}/test
  ${CMAKE_CURRENT_BINARY_DIR}/test)

set(tests
  test/address.cpp
  test/batch.cpp
  test/binner.cpp
  test/bitmap.cpp
  test/bitmap_index.cpp
  test/bits.cpp
  test/bitvector.cpp
  test/byte.cpp
  test/cache.cpp
  test/chunk.cpp
  test/coder.cpp
  test/command.cpp
  test/compressedbuf.cpp
  test/data.cpp
  test/endpoint.cpp
  test/event.cpp
  test/expression.cpp
  test/expression_evaluation.cpp
  test/expression_parseable.cpp
  test/filesystem.cpp
  test/fixtures/events.cpp
  test/format/bro.cpp
  test/format/mrt.cpp
  test/format/writer.cpp
  test/hash.cpp
  test/http.cpp
  test/ids.cpp
  test/iterator.cpp
  test/json.cpp
  test/key.cpp
  test/main.cpp
  test/mmapbuf.cpp
  test/offset.cpp
  test/option_declaration_set.cpp
  test/option_map.cpp
  test/parse_data.cpp
  test/parseable.cpp
  test/pattern.cpp
  test/port.cpp
  test/printable.cpp
  test/range_map.cpp
  test/save_load.cpp
  test/schema.cpp
  test/serialization.cpp
  test/span.cpp
  test/stack.cpp
  test/string.cpp
  test/subnet.cpp
  test/system/archive.cpp
  test/system/column_index.cpp
  test/system/consensus.cpp
  test/system/exporter.cpp
  test/system/importer.cpp
  test/system/index.cpp
  test/system/indexer.cpp
  test/system/indexer_manager.cpp
  test/system/indexer_stage_driver.cpp
  test/system/key_value_store.cpp
  test/system/partition.cpp
  test/system/queries.cpp
  test/system/replicated_store.cpp
  test/system/sink.cpp
  test/system/source.cpp
  test/system/table_index.cpp
  test/system/task.cpp
  test/time.cpp
  test/type.cpp
  test/uuid.cpp
  test/value.cpp
  test/value_index.cpp
  test/variant.cpp
  test/vector_map.cpp
  test/vector_set.cpp
  test/view.cpp
  test/word.cpp
)

if (PCAP_FOUND)
  set(tests ${tests} test/format/pcap.cpp)
endif ()

if (VAST_USE_OPENCL)
  set(tests ${tests} test/system/opencl.cpp)
endif ()

add_executable(vast-test ${tests})
target_link_libraries(vast-test libvast ${CMAKE_THREAD_LIBS_INIT})

#
# Generate CMake tests
#

# Helper macro to construct a CMake test from a VAST test suite.
macro(make_test suite)
  string(REPLACE " " "_" test_name ${suite})
  set(vast_test ${EXECUTABLE_OUTPUT_PATH}/vast-test)
  add_test(${test_name} ${vast_test} -v 3 -r 60 -s "^${suite}$" ${ARGN})
endmacro()

# Find all test suites.
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

enable_testing()

foreach(suite ${suites})
  make_test("${suite}")
endforeach ()
