configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
               ${CMAKE_CURRENT_BINARY_DIR}/config.h)

set(libvast_sources
  ../vast.cc
  address.cc
  archive.cc
  bitvector.cc
  bitstream.cc
  chunk.cc
  configuration.cc
  expression.cc
  event.cc
  error.cc
  file_system.cc
  id_tracker.cc
  index.cc
  individual.cc
  ingestor.cc
  logger.cc
  object.cc
  operator.cc
  partition.cc
  port.cc
  prefix.cc
  profiler.cc
  program.cc
  query.cc
  receiver.cc
  regex.cc
  schema.cc
  search.cc
  segment.cc
  segment_pack.cc
  segmentizer.cc
  serialization.cc
  signal_monitor.cc
  string.cc
  time.cc
  type.cc
  type_info.cc
  type_tag.cc
  uuid.cc
  value.cc
  detail/cppa_serialization.cc
  detail/demangle.cc
  detail/type_manager.cc
  detail/ast/query.cc
  io/array_stream.cc
  io/buffered_stream.cc
  io/coded_stream.cc
  io/compressed_stream.cc
  io/compression.cc
  io/file_stream.cc
  io/getline.cc
  io/stream.cc
  source/file.cc
  util/poll.cc
  util/terminal.cc
  util/hash/crc.cc
  util/hash/murmur.cc
  util/hash/xxhash.cc
  )

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

set(libvast_libs lz4 ${LIBCPPA_LIBRARIES})

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
