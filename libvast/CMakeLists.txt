# ----------------------------------------------------------------------------
# flattbuffers
# ----------------------------------------------------------------------------

file(GLOB flatbuffers_schemas CONFIGURE_DEPENDS
     ${CMAKE_CURRENT_SOURCE_DIR}/vast/fbs/*.fbs)
set(flatbuffers_output_path ${CMAKE_CURRENT_BINARY_DIR}/vast/fbs)

add_custom_target(libvast_flatbuffers)

# Translate paths to desired output paths.
foreach (schema ${flatbuffers_schemas})
  get_filename_component(basename ${schema} NAME_WE)
  # The hardcoded path that flatc generates.
  set(output_file "${flatbuffers_output_path}/${basename}_generated.h")
  # The path that we want.
  set(desired_file "${flatbuffers_output_path}/${basename}.hpp")
  # Hackish way to patch generated flatbuffers schemas to support our naming.
  set(rename_${basename}
      ${CMAKE_CURRENT_BINARY_DIR}/flatbuffers_strip_suffix_${basename}.cmake)
  file(
    WRITE ${rename_${basename}}
    "file(READ \"${desired_file}\" include)\n"
    "string(REGEX REPLACE\n"
    "      \"([^\\n]+)_generated.h\\\"\"\n"
    "      \"\\\\1.hpp\\\"\"\n"
    "      new_include \"\${include}\")\n"
    "file(WRITE \"${desired_file}\" \"\${new_include}\")\n")
  # Compile and rename schema.
  add_custom_command(
    OUTPUT ${desired_file}
    COMMAND
      flatbuffers::flatc -b --cpp --scoped-enums -I
      ${CMAKE_CURRENT_SOURCE_DIR}/flatbuffers -o ${flatbuffers_output_path}
      ${schema}
    COMMAND ${CMAKE_COMMAND} -E rename ${output_file} ${desired_file}
    COMMAND ${CMAKE_COMMAND} -P ${rename_${basename}}
    DEPENDS ${schema}
    COMMENT "Compiling flatbuffers schema ${schema}")
  add_custom_target(flatbuffers_${basename} DEPENDS ${desired_file})
  add_dependencies(libvast_flatbuffers flatbuffers_${basename})
endforeach ()

# ----------------------------------------------------------------------------
# libvast
# ----------------------------------------------------------------------------

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/vast/config.hpp.in
               ${CMAKE_CURRENT_BINARY_DIR}/vast/config.hpp)

# Some CMake generators (e.g., XCode) require header files in addition to the
# sources.
file(GLOB_RECURSE libvast_headers "${CMAKE_CURRENT_SOURCE_DIR}/vast/*.hpp")

set(libvast_sources
    src/accountant/config.cpp
    src/address.cpp
    src/attribute.cpp
    src/banner.cpp
    src/base.cpp
    src/bitmap.cpp
    src/bloom_filter_parameters.cpp
    src/bloom_filter_synopsis.cpp
    src/bool_synopsis.cpp
    src/caf_table_slice.cpp
    src/caf_table_slice_builder.cpp
    src/chunk.cpp
    src/command.cpp
    src/compression.cpp
    src/concept/hashable/crc.cpp
    src/concept/hashable/sha1.cpp
    src/concept/hashable/xxhash.cpp
    src/concept/parseable/vast/expression.cpp
    src/data.cpp
    src/defaults.cpp
    src/detail/add_message_types.cpp
    src/detail/base64.cpp
    src/detail/compressedbuf.cpp
    src/detail/fdinbuf.cpp
    src/detail/fdistream.cpp
    src/detail/fdostream.cpp
    src/detail/fdoutbuf.cpp
    src/detail/fill_status_map.cpp
    src/detail/line_range.cpp
    src/detail/make_io_stream.cpp
    src/detail/mmapbuf.cpp
    src/detail/pid_file.cpp
    src/detail/posix.cpp
    src/detail/process.cpp
    src/detail/string.cpp
    src/detail/system.cpp
    src/detail/terminal.cpp
    src/die.cpp
    src/directory.cpp
    src/error.cpp
    src/ether_type.cpp
    src/event_types.cpp
    src/ewah_bitmap.cpp
    src/expression.cpp
    src/expression_visitors.cpp
    src/fbs/utils.cpp
    src/file.cpp
    src/flow.cpp
    src/format/ascii.cpp
    src/format/csv.cpp
    src/format/json.cpp
    src/format/multi_layout_reader.cpp
    src/format/null.cpp
    src/format/ostream_writer.cpp
    src/format/reader.cpp
    src/format/single_layout_reader.cpp
    src/format/syslog.cpp
    src/format/test.cpp
    src/format/writer.cpp
    src/format/zeek.cpp
    src/http.cpp
    src/icmp.cpp
    src/ids.cpp
    src/io/read.cpp
    src/io/save.cpp
    src/io/write.cpp
    src/json.cpp
    src/logger.cpp
    src/meta_index.cpp
    src/msgpack.cpp
    src/msgpack_table_slice.cpp
    src/msgpack_table_slice_builder.cpp
    src/null_bitmap.cpp
    src/operator.cpp
    src/path.cpp
    src/pattern.cpp
    src/port.cpp
    src/qualified_record_field.cpp
    src/schema.cpp
    src/segment.cpp
    src/segment_builder.cpp
    src/segment_store.cpp
    src/settings.cpp
    src/store.cpp
    src/subnet.cpp
    src/synopsis.cpp
    src/synopsis_factory.cpp
    src/system/accountant.cpp
    src/system/application.cpp
    src/system/archive.cpp
    src/system/configuration.cpp
    src/system/connect_to_node.cpp
    src/system/component_registry.cpp
    src/system/count_command.cpp
    src/system/counter.cpp
    src/system/default_configuration.cpp
    src/system/eraser.cpp
    src/system/evaluator.cpp
    src/system/explore_command.cpp
    src/system/explorer.cpp
    src/system/exporter.cpp
    src/system/importer.cpp
    src/system/index.cpp
    src/system/indexer.cpp
    src/system/infer_command.cpp
    src/system/make_sink.cpp
    src/system/node.cpp
    src/system/partition.cpp
    src/system/pivot_command.cpp
    src/system/pivoter.cpp
    src/system/posix_filesystem.cpp
    src/system/query_processor.cpp
    src/system/query_supervisor.cpp
    src/system/read_query.cpp
    src/system/remote_command.cpp
    src/system/shutdown.cpp
    src/system/signal_monitor.cpp
    src/system/sink_command.cpp
    src/system/spawn_archive.cpp
    src/system/spawn_arguments.cpp
    src/system/spawn_counter.cpp
    src/system/spawn_eraser.cpp
    src/system/spawn_explorer.cpp
    src/system/spawn_exporter.cpp
    src/system/spawn_importer.cpp
    src/system/spawn_index.cpp
    src/system/spawn_node.cpp
    src/system/spawn_or_connect_to_node.cpp
    src/system/spawn_pivoter.cpp
    src/system/spawn_sink.cpp
    src/system/spawn_type_registry.cpp
    src/system/start_command.cpp
    src/system/stop_command.cpp
    src/system/task.cpp
    src/system/terminator.cpp
    src/system/type_registry.cpp
    src/system/version_command.cpp
    src/table_slice.cpp
    src/table_slice_builder.cpp
    src/table_slice_builder_factory.cpp
    src/table_slice_factory.cpp
    src/time.cpp
    src/time_synopsis.cpp
    src/type.cpp
    src/uuid.cpp
    src/value_index.cpp
    src/value_index_factory.cpp
    src/view.cpp
    src/wah_bitmap.cpp)

if (VAST_HAVE_ARROW)
  set(libvast_sources ${libvast_sources} src/arrow_table_slice.cpp
                      src/arrow_table_slice_builder.cpp src/format/arrow.cpp)
endif ()

if (PCAP_FOUND)
  set(libvast_sources ${libvast_sources} src/system/pcap_writer_command.cpp
                      src/format/pcap.cpp)
endif ()

add_library(libvast ${libvast_sources} ${libvast_headers})
set_target_properties(
  libvast
  PROPERTIES SOVERSION "${VERSION_YEAR}" VERSION
                                         "${VERSION_YEAR}.${VERSION_MONTH}"
             OUTPUT_NAME vast)

target_include_directories(
  libvast
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_include_directories(libvast PRIVATE "${PROJECT_SOURCE_DIR}/aux")

add_dependencies(libvast vast-schema)

target_link_libraries(libvast PUBLIC tsl::robin_map)

target_link_libraries(libvast PUBLIC caf::core caf::io)
if (VAST_USE_OPENSSL)
  target_link_libraries(libvast PUBLIC caf::openssl)
endif ()

target_link_libraries(libvast PUBLIC ${flatbuffers_target})
add_dependencies(libvast libvast_flatbuffers)

if (VAST_ENABLE_ASSERTIONS)
  target_include_directories(libvast PUBLIC ${Backtrace_INCLUDE_DIRECTORIES})
  target_link_libraries(libvast PUBLIC ${Backtrace_LIBRARIES})
endif ()

if (VAST_HAVE_ARROW)
  target_link_libraries(libvast PUBLIC ${ARROW_LIBRARY})
endif ()

if (PCAP_FOUND)
  target_link_libraries(libvast PRIVATE pcap::pcap)
endif ()

if (VAST_USE_JEMALLOC)
  target_link_libraries(libvast PRIVATE jemalloc::jemalloc_)
endif ()

if (BUILD_SHARED_LIBS AND "${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
  target_link_libraries(libvast PUBLIC dl)
endif ()

# Install libvast in PREFIX/lib and headers in PREFIX/include/vast.
install(
  TARGETS libvast
  EXPORT VASTTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(
  DIRECTORY vast
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  FILES_MATCHING
  PATTERN "*.hpp")

# Install generated config and flatbuffers headers.
install(
  DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/vast"
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  FILES_MATCHING
  PATTERN "*.hpp")

add_library(vast::libvast ALIAS libvast)

# ----------------------------------------------------------------------------
# unit tests
# ----------------------------------------------------------------------------

set(tests
    test/address.cpp
    test/address_synopsis.cpp
    test/binner.cpp
    test/bitmap.cpp
    test/bitmap_algorithms.cpp
    test/bitmap_index.cpp
    test/bits.cpp
    test/bitvector.cpp
    test/bloom_filter.cpp
    test/bloom_filter_synopsis.cpp
    test/byte.cpp
    test/cache.cpp
    test/chunk.cpp
    test/coder.cpp
    test/command.cpp
    test/community_id.cpp
    test/compressedbuf.cpp
    test/data.cpp
    test/detail/algorithms.cpp
    test/detail/base64.cpp
    test/detail/column_iterator.cpp
    test/detail/flat_lru_cache.cpp
    test/detail/flat_map.cpp
    test/detail/operators.cpp
    test/detail/set_operations.cpp
    test/endpoint.cpp
    test/error.cpp
    test/expression.cpp
    test/expression_evaluation.cpp
    test/expression_parseable.cpp
    test/factory.cpp
    test/filesystem.cpp
    test/flow.cpp
    test/format/csv.cpp
    test/format/json.cpp
    test/format/syslog.cpp
    test/format/writer.cpp
    test/format/zeek.cpp
    test/hash.cpp
    test/hash_index.cpp
    test/http.cpp
    test/ids.cpp
    test/iterator.cpp
    test/json.cpp
    test/meta_index.cpp
    test/mmapbuf.cpp
    test/msgpack.cpp
    test/msgpack_table_slice.cpp
    test/offset.cpp
    test/parse_data.cpp
    test/parseable.cpp
    test/pattern.cpp
    test/port.cpp
    test/printable.cpp
    test/range_map.cpp
    test/save_load.cpp
    test/schema.cpp
    test/scope_linked.cpp
    test/segment.cpp
    test/segment_store.cpp
    test/span.cpp
    test/stack.cpp
    test/string.cpp
    test/subnet.cpp
    test/synopsis.cpp
    test/system/archive.cpp
    test/system/counter.cpp
    test/system/datagram_source.cpp
    test/system/eraser.cpp
    test/system/evaluator.cpp
    test/system/explorer.cpp
    test/system/exporter.cpp
    test/system/filesystem.cpp
    test/system/importer.cpp
    test/system/index.cpp
    test/system/pivoter.cpp
    test/system/queries.cpp
    test/system/query_processor.cpp
    test/system/query_supervisor.cpp
    test/system/sink.cpp
    test/system/source.cpp
    test/system/task.cpp
    test/system/terminate.cpp
    test/system/type_registry.cpp
    test/table_slice.cpp
    test/time.cpp
    test/type.cpp
    test/uuid.cpp
    test/value_index.cpp
    test/vector_map.cpp
    test/vector_set.cpp
    test/view.cpp
    test/word.cpp)

if (VAST_HAVE_ARROW)
  set(tests ${tests} test/format/arrow.cpp test/arrow_table_slice.cpp)
endif ()

if (PCAP_FOUND)
  set(tests ${tests} test/format/pcap.cpp)
endif ()

#
# Generate CMake tests
#

if (BUILD_UNIT_TESTS)
  # Add vast-test executable.
  add_executable(vast-test ${tests})
  target_link_libraries(vast-test libvast_test libvast
                        ${CMAKE_THREAD_LIBS_INIT})

  if (VAST_HAVE_ARROW)
    target_link_libraries(vast-test ${ARROW_LIBRARY})
  endif ()

  if (PCAP_FOUND)
    target_link_libraries(vast-test pcap::pcap)
  endif ()

  add_test(NAME build_vast_test
           COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --config
                   "$<CONFIG>" --target vast-test)
  set_tests_properties(build_vast_test PROPERTIES FIXTURES_SETUP
                                                  vast_unit_test_fixture)
  # Helper macro to construct a CMake test from a VAST test suite.
  macro (make_test suite)
    string(REPLACE " " "_" test_name ${suite})
    add_test(NAME ${test_name} COMMAND vast-test -v 3 -r 600 -s "^${suite}$"
                                       ${ARGN})
    set_tests_properties(${test_name} PROPERTIES FIXTURES_REQUIRED
                                                 vast_unit_test_fixture)
  endmacro ()
  # Find all test suites.
  foreach (test ${tests})
    file(STRINGS ${test} contents)
    foreach (line ${contents})
      if ("${line}" MATCHES "SUITE")
        string(REGEX REPLACE "#define SUITE \(.*\)" "\\1" suite ${line})
        list(APPEND suites ${suite})
      endif ()
    endforeach ()
  endforeach ()
  list(REMOVE_DUPLICATES suites)
  # Enable unit testing via CMake/CTest and add all test suites.
  foreach (suite ${suites})
    make_test("${suite}")
  endforeach ()
endif ()
