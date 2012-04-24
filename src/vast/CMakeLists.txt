include_directories(..)

set(libs
    ${0event_LIBRARIES}
    ${Boost_LIBRARIES}
    ${Broccoli_LIBRARIES}
    ${ZeroMQ_LIBRARIES})

if (USE_PERFTOOLS)
    set(libs ${libs} ${Perftools_LIBRARIES})
endif ()

# Everything except main.cc
file(GLOB_RECURSE vast_lib_sources
    configuration.cc
    exception.cc
    program.cc
    */*.cc)

add_library(libvast SHARED ${vast_lib_sources})
set_target_properties(libvast PROPERTIES OUTPUT_NAME vast)
target_link_libraries(libvast ${libs})
install(TARGETS libvast DESTINATION lib)

add_executable(vast main.cc)
set_target_properties(vast PROPERTIES OUTPUT_NAME vast)
target_link_libraries(vast libvast ${libs})
install(TARGETS vast DESTINATION bin)
