include_directories(..)

set(libs
    ${0event_LIBRARIES}
    ${Boost_LIBRARIES}
    ${Broccoli_LIBRARIES}
    ${ZeroMQ_LIBRARIES})

if (USE_PERFTOOLS)
    set(libs ${libs} ${Perftools_LIBRARIES})
endif ()

file(GLOB_RECURSE vast_bin_sources *.cc)
add_executable(vast-bin ${vast_bin_sources})
set_target_properties(vast-bin PROPERTIES OUTPUT_NAME vast)
target_link_libraries(vast-bin ${libs})
install(TARGETS vast-bin DESTINATION bin)

file(GLOB_RECURSE vast_lib_sources
    component.cc
    configuration.cc
    exception.cc
    program.cc
    */*.cc)

add_library(vast-lib SHARED ${vast_lib_sources})
set_target_properties(vast-lib PROPERTIES OUTPUT_NAME vast)
target_link_libraries(vast-lib ${libs})
install(TARGETS vast-lib DESTINATION lib)
