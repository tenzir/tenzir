include_directories(..)

set(libs ${Boost_LIBRARIES} ${Broccoli_LIBRARIES} ${0event_LIBRARIES})
if (USE_PERFTOOLS)
    set(libs ${libs} ${Perftools_LIBRARIES})
endif ()

file(GLOB_RECURSE vast_exe_sources *.cc)
add_executable(vnode ${vast_exe_sources})
target_link_libraries(vnode ${libs})
install(TARGETS vnode DESTINATION bin)

file(GLOB_RECURSE vast_lib_sources
    configuration.cc
    exception.cc
    program.cc
    */*.cc)

add_library(vast SHARED ${vast_lib_sources})
target_link_libraries(vast ${libs})
install(TARGETS vast DESTINATION lib)
