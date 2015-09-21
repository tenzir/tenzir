configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
               ${CMAKE_CURRENT_BINARY_DIR}/config.h)

set(libvast_sources
  address.cc
  announce.cc
  banner.cc
  bitvector.cc
  bitstream.cc
  bitstream_polymorphic.cc
  block.cc
  chunk.cc
  cleanup.cc
  data.cc
  die.cc
  expression.cc
  event.cc
  filesystem.cc
  individual.cc
  logger.cc
  operator.cc
  pattern.cc
  port.cc
  schema.cc
  subnet.cc
  time.cc
  type.cc
  uuid.cc
  value.cc
  actor/accountant.cc
  actor/archive.cc
  actor/controller.cc
  actor/exporter.cc
  actor/importer.cc
  actor/identifier.cc
  actor/index.cc
  actor/key_value_store.cc
  actor/node.cc
  actor/partition.cc
  actor/signal_monitor.cc
  actor/task.cc
  actor/sink/ascii.cc
  actor/sink/bro.cc
  actor/sink/json.cc
  actor/sink/spawn.cc
  actor/source/bro.cc
  actor/source/bgpdump.cc
  actor/source/spawn.cc
  actor/source/test.cc
  concept/convertible/vast/address.cc
  concept/convertible/vast/data.cc
  concept/convertible/vast/event.cc
  concept/convertible/vast/pattern.cc
  concept/convertible/vast/port.cc
  concept/convertible/vast/subnet.cc
  concept/convertible/vast/time.cc
  concept/convertible/vast/value.cc
  concept/serializable/hierarchy.cc
  concept/parseable/vast/detail/to_expression.cc
  concept/parseable/vast/detail/to_schema.cc
  concept/parseable/vast/detail/query_ast.cc
  detail/adjust_resource_consumption.cc
  expr/evaluator.cc
  expr/normalize.cc
  expr/predicatizer.cc
  expr/restrictor.cc
  expr/resolver.cc
  expr/validator.cc
  io/actor_stream.cc
  io/algorithm.cc
  io/array_stream.cc
  io/buffered_stream.cc
  io/coded_stream.cc
  io/compressed_stream.cc
  io/device.cc
  io/file_stream.cc
  io/getline.cc
  io/iterator.cc
  io/stream_device.cc
  io/stream.cc
  util/endpoint.cc
  util/fdistream.cc
  util/fdinbuf.cc
  util/fdostream.cc
  util/fdoutbuf.cc
  util/posix.cc
  util/string.cc
  util/system.cc
  util/terminal.cc
  util/detail/posix.cc
  util/hash/crc.cc
  util/hash/murmur.cc
  util/hash/xxhash.cc
)

if (GPERFTOOLS_FOUND)
  set(libvast_sources ${libvast_sources} actor/profiler.cc)
endif ()

if (PCAP_FOUND)
  set(libvast_sources ${libvast_sources}
    actor/sink/pcap.cc
    actor/source/pcap.cc)
endif ()

set(libvast_libs lz4 ${CAF_LIBRARIES})

if (VAST_ENABLE_ASSERTIONS)
  set(libvast_libs ${libvast_libs} ${Backtrace_LIBRARIES})
endif ()

if (PCAP_FOUND)
  set(libvast_libs ${libvast_libs} ${PCAP_LIBRARIES})
endif ()

if (BROCCOLI_FOUND)
  set(libvast_libs ${libvast_libs} ${BROCCOLI_LIBRARIES})
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
install(TARGETS libvast DESTINATION lib)
