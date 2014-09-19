include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(vast)

add_executable(vast main.cc)
set_target_properties(vast PROPERTIES OUTPUT_NAME vast)
target_link_libraries(vast libvast ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS vast DESTINATION bin)
