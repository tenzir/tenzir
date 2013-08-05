include_directories(BEFORE ..)

set(libvast_sources
  address.cc
  archive.cc
  bitvector.cc
  bitstream.cc
  configuration.cc
  container.cc
  exception.cc
  expression.cc
  event.cc
  file_system.cc
  id_tracker.cc
  index.cc
  individual.cc
  ingestor.cc
  logger.cc
  object.cc
  operator.cc
  parse.cc
  port.cc
  prefix.cc
  program.cc
  query.cc
  query_client.cc
  regex.cc
  schema.cc
  schema_manager.cc
  search.cc
  segment.cc
  segment_manager.cc
  serialization.cc
  shutdown.cc
  string.cc
  system_monitor.cc
  time.cc
  to_string.cc
  type_info.cc
  uuid.cc
  value.cc
  value_type.cc
  detail/cppa_serialization.cc
  detail/cppa_type_info.cc
  detail/demangle.cc
  detail/type_manager.cc
  detail/parse.cc
  detail/ast/query.cc
  detail/parser/address.cc
  detail/parser/expression.cc
  detail/parser/duration.cc
  detail/parser/query.cc
  detail/parser/time_point.cc
  detail/parser/schema.cc
  detail/parser/value.cc
  detail/parser/bro15/conn.cc
  io/array_stream.cc
  io/buffered_stream.cc
  io/coded_stream.cc
  io/compressed_stream.cc
  io/compression.cc
  io/file_stream.cc
  io/stream.cc
  sink/file.cc
  sink/asynchronous.cc
  sink/segmentizer.cc
  source/file.cc
  source/synchronous.cc
  util/configuration.cc
  util/console.cc
  util/poll.cc
  util/profiler.cc
  )

if (BROCCOLI_FOUND)
  set(libvast_sources ${libvast_sources}
    source/broccoli.cc
    util/broccoli.cc)
endif ()

set(libvast_libs lz4 ${LIBCPPA_LIBRARIES})

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
set_target_properties(libvast PROPERTIES OUTPUT_NAME vast)
target_link_libraries(libvast ${libvast_libs})
install(TARGETS libvast DESTINATION lib)

add_executable(vast main.cc)
set_target_properties(vast PROPERTIES OUTPUT_NAME vast)
target_link_libraries(vast libvast)
install(TARGETS vast DESTINATION bin)
