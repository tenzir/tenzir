include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(vast)

add_executable(vast vast.cc)
target_link_libraries(vast libvast ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS vast DESTINATION bin)
