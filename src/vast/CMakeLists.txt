configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
               ${CMAKE_CURRENT_BINARY_DIR}/config.h)

set(libvast_sources
  ../vast.cc
  address.cc
  archive.cc
  bitvector.cc
  bitstream.cc
  block.cc
  chunk.cc
  configuration.cc
  data.cc
  expression.cc
  event.cc
  error.cc
  exporter.cc
  file_system.cc
  identifier.cc
  index.cc
  individual.cc
  importer.cc
  logger.cc
  operator.cc
  partition.cc
  pattern.cc
  port.cc
  profiler.cc
  program.cc
  query.cc
  receiver.cc
  schema.cc
  search.cc
  serialization.cc
  signal_monitor.cc
  subnet.cc
  time.cc
  tracker.cc
  type.cc
  uuid.cc
  value.cc
  detail/caf_serialization.cc
  detail/type_manager.cc
  detail/ast/query.cc
  expr/evaluator.cc
  expr/hoister.cc
  expr/normalizer.cc
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
  sink/bro.cc
  sink/chunkifier.cc
  sink/json.cc
  source/bro.cc
  source/test.cc
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
    sink/pcap.cc
    source/pcap.cc)
endif ()

if (BROCCOLI_FOUND)
  set(libvast_sources ${libvast_sources}
    source/broccoli.cc
    util/broccoli.cc)
endif ()

if (EDITLINE_FOUND)
  set(libvast_sources ${libvast_sources}
    console.cc
    util/command_line.cc
    util/editline.cc)
endif ()

set(libvast_libs lz4 ${LIBCAF_LIBRARIES})

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
