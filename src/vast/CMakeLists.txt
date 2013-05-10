include_directories(BEFORE ..)

set(libvast_sources
  bitvector.cc
  bitstream.cc
  archive.cc
  configuration.cc
  exception.cc
  expression.cc
  id_tracker.cc
  index.cc
  ingestor.cc
  logger.cc
  operator.cc
  program.cc
  query.cc
  query_client.cc
  schema.cc
  schema_manager.cc
  search.cc
  segment.cc
  segmentizer.cc
  segment_manager.cc
  system_monitor.cc
  to_string.cc
  detail/cppa_serialization.cc
  detail/cppa_type_info.cc
  detail/ast/query.cc
  detail/parser/expression.cc
  detail/parser/query.cc
  detail/parser/schema.cc
  detail/parser/bro15/conn.cc
  sink/file.cc
  sink/synchronous.cc
  source/file.cc
  source/synchronous.cc
  util/configuration.cc
  util/console.cc
  util/poll.cc
  util/profiler.cc)

if (BROCCOLI_FOUND)
  set(libvast_sources ${libvast_sources}
    source/broccoli.cc
    util/broccoli.cc)
endif ()

set(libvast_libs ${LIBCPPA_LIBRARIES} ${0EVENT_LIBRARIES})

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
