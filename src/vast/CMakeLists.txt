include_directories(BEFORE ..)

set(vast_lib_sources
  configuration.cc
  event_source.cc
  exception.cc
  id_tracker.cc
  ingestor.cc
  logger.cc
  program.cc
  comm/broccoli.cc
  comm/connection.cc
  comm/exception.cc
  comm/server.cc
  detail/cppa_archive.cc
  detail/cppa_type_info.cc
  fs/exception.cc
  fs/operations.cc
  meta/argument.cc
  meta/event.cc
  meta/exception.cc
  meta/parser/taxonomy.cc
  meta/schema_manager.cc
  meta/taxonomy.cc
  meta/type.cc
  query/ast.cc
  query/client.cc
  query/exception.cc
  query/expression.cc
  query/parser/expression.cc
  query/parser/query.cc
  query/query.cc
  query/search.cc
  source/broccoli.cc
  source/file.cc
  store/archive.cc
  store/emitter.cc
  store/exception.cc
  store/index.cc
  store/segment.cc
  store/segment_manager.cc
  store/segmentizer.cc
  util/console.cc
  util/profiler.cc)

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

add_library(libvast SHARED ${vast_lib_sources})
set_target_properties(libvast PROPERTIES OUTPUT_NAME vast)
target_link_libraries(libvast ${libs})
install(TARGETS libvast DESTINATION lib)

add_executable(vast main.cc)
set_target_properties(vast PROPERTIES OUTPUT_NAME vast)
target_link_libraries(vast libvast ${libs})
install(TARGETS vast DESTINATION bin)
