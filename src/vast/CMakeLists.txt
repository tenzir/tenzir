configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
               ${CMAKE_CURRENT_BINARY_DIR}/config.h)

set(libvast_sources
  address.cc
  announce.cc
  bitvector.cc
  bitstream.cc
  bitstream_polymorphic.cc
  block.cc
  chunk.cc
  cleanup.cc
  configuration.cc
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
  actor/archive.cc
  actor/exporter.cc
  actor/identifier.cc
  actor/index.cc
  actor/partition.cc
  actor/profiler.cc
  actor/program.cc
  actor/query.cc
  actor/receiver.cc
  actor/search.cc
  actor/signal_monitor.cc
  actor/tracker.cc
  actor/sink/ascii.cc
  actor/sink/bro.cc
  actor/sink/json.cc
  actor/source/bro.cc
  actor/source/bgpdump.cc
  actor/source/test.cc
  concept/serializable/hierarchy.cc
  detail/ast/query.cc
  expr/evaluator.cc
  expr/normalize.cc
  expr/predicatizer.cc
  expr/restrictor.cc
  expr/resolver.cc
  expr/validator.cc
  io/algorithm.cc
  io/array_stream.cc
  io/buffered_stream.cc
  io/coded_stream.cc
  io/compressed_stream.cc
  io/file_stream.cc
  io/getline.cc
  io/iostream.cc
  io/stream.cc
  util/poll.cc
  util/string.cc
  util/system.cc
  util/terminal.cc
  util/hash/crc.cc
  util/hash/murmur.cc
  util/hash/xxhash.cc
  )

if (PCAP_FOUND)
  set(libvast_sources ${libvast_sources}
    actor/sink/pcap.cc
    actor/source/pcap.cc)
endif ()

if (EDITLINE_FOUND)
  set(libvast_sources ${libvast_sources}
    actor/console.cc
    util/command_line.cc
    util/editline.cc)
endif ()

set(libvast_libs lz4 ${CAF_LIBRARIES})

if (PCAP_FOUND)
  set(libvast_libs ${libvast_libs} ${PCAP_LIBRARIES})
endif ()

if (BROCCOLI_FOUND)
  set(libvast_libs ${libvast_libs} ${BROCCOLI_LIBRARIES})
endif ()

if (EDITLINE_FOUND)
  set(libvast_libs ${libvast_libs} ${EDITLINE_LIBRARIES})
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
