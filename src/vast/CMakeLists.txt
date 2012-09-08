include_directories(BEFORE ..)

set(libvast_sources
  archive.cc
  configuration.cc
  event_sink.cc
  event_source.cc
  exception.cc
  expression.cc
  id_tracker.cc
  index.cc
  ingestor.cc
  logger.cc
  program.cc
  query.cc
  query_client.cc
  schema.cc
  schema_manager.cc
  search.cc
  segment.cc
  segment_manager.cc
  system_monitor.cc
  to_string.cc
  detail/cppa_archive.cc
  detail/cppa_type_info.cc
  detail/ast/query.cc
  detail/parser/expression.cc
  detail/parser/query.cc
  detail/parser/schema.cc
  fs/exception.cc
  fs/operations.cc
  sink/file.cc
  source/broccoli.cc
  source/file.cc
  util/broccoli.cc
  util/console.cc
  util/poll.cc
  util/profiler.cc
  )

set(libs
    ${Boost_LIBRARIES}
    ${LIBCPPA_LIBRARIES}
    ${0event_LIBRARIES}
    ${Broccoli_LIBRARIES})

# Always link with -lprofile if we have Gperftools.
if (GPERFTOOLS_FOUND)
  set(libs ${libs} ${GPERFTOOLS_PROFILER})
endif ()

# Only link against tcmalloc if requested.
if (GPERFTOOLS_FOUND AND USE_PERFTOOLS_HEAP_PROFILER)
  set(libs ${libs} ${GPERFTOOLS_TCMALLOC})
endif ()

add_library(libvast SHARED ${libvast_sources})
set_target_properties(libvast PROPERTIES OUTPUT_NAME vast)
target_link_libraries(libvast ${libs})
install(TARGETS libvast DESTINATION lib)

add_executable(vast main.cc)
set_target_properties(vast PROPERTIES OUTPUT_NAME vast)
target_link_libraries(vast libvast ${libs})
install(TARGETS vast DESTINATION bin)
