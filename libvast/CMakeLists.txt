configure_file(${CMAKE_CURRENT_SOURCE_DIR}/vast/config.hpp.in
               ${CMAKE_CURRENT_BINARY_DIR}/vast/config.hpp)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(libvast_sources
  ${aux_sources}
  src/address.cpp
  src/attribute.cpp
  src/banner.cpp
  src/base.cpp
  src/batch.cpp
  src/bitmap.cpp
  src/compression.cpp
  src/data.cpp
  src/die.cpp
  src/expression.cpp
  src/expression_visitors.cpp
  src/error.cpp
  src/event.cpp
  src/ewah_bitmap.cpp
  src/filesystem.cpp
  src/key.cpp
  src/http.cpp
  src/null_bitmap.cpp
  src/operator.cpp
  src/pattern.cpp
  src/port.cpp
  src/schema.cpp
  src/subnet.cpp
  src/time.cpp
  src/type.cpp
  src/uuid.cpp
  src/value.cpp
  src/value_index.cpp
  src/concept/hashable/crc.cpp
  src/concept/hashable/xxhash.cpp
  src/detail/adjust_resource_consumption.cpp
  src/detail/compressedbuf.cpp
  src/detail/line_range.cpp
  src/detail/fdistream.cpp
  src/detail/fdinbuf.cpp
  src/detail/fdostream.cpp
  src/detail/fdoutbuf.cpp
  src/detail/mmapbuf.cpp
  src/detail/posix.cpp
  src/detail/string.cpp
  src/detail/system.cpp
  src/detail/terminal.cpp
  src/system/accountant.cpp
  src/system/archive.cpp
  src/system/configuration.cpp
  src/system/consensus.cpp
  src/system/exporter.cpp
  src/system/importer.cpp
  #src/system/identifier.cpp
  src/system/index.cpp
  src/system/indexer.cpp
  #src/system/node.cpp
  src/system/partition.cpp
  src/system/profiler.cpp
  src/system/signal_monitor.cpp
  src/system/task.cpp
  src/format/bgpdump.cpp
  src/format/bro.cpp
  src/format/test.cpp
)

if (PCAP_FOUND)
  set(libvast_sources ${libvast_sources}
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

add_library(libvast SHARED ${libvast_sources})
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

# ----------------------------------------------------------------------------
#                                 Unit Tests
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
  test/bitvector.cpp
  test/cache.cpp
  test/coder.cpp
  test/compressedbuf.cpp
  test/data.cpp
  test/date.cpp
  test/endpoint.cpp
  test/event.cpp
  test/expression.cpp
  test/expression_evaluation.cpp
  test/expression_parseable.cpp
  test/filesystem.cpp
  test/flat_serial_set.cpp
  test/hash.cpp
  test/http.cpp
  test/iterator.cpp
  test/json.cpp
  test/key.cpp
  test/main.cpp
  test/maybe.cpp
  test/mmapbuf.cpp
  test/offset.cpp
  test/parseable.cpp
  test/pattern.cpp
  test/port.cpp
  test/printable.cpp
  test/range_map.cpp
  test/save_load.cpp
  test/schema.cpp
  test/stack.cpp
  test/string.cpp
  test/subnet.cpp
  test/time.cpp
  test/type.cpp
  test/uuid.cpp
  test/value.cpp
  test/value_index.cpp
  test/variant.cpp
  test/word.cpp
  test/system/archive.cpp
  test/system/consensus.cpp
  #test/system/export.cpp
  test/system/exporter.cpp
  #test/system/import.cpp
  test/system/index.cpp
  test/system/indexer.cpp
  test/system/key_value_store.cpp
  test/system/partition.cpp
  test/system/replicated_store.cpp
  test/system/sink.cpp
  test/system/source.cpp
  test/system/task.cpp
  test/format/bro.cpp
  test/format/writer.cpp
)

if (PCAP_FOUND)
  set(tests ${tests}
    test/format/pcap.cpp)
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
