include_directories(BEFORE ..)

# Everything except main.cc
file(GLOB_RECURSE vast_lib_sources
    configuration.cc
    exception.cc
    program.cc
    */*.cc)

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
