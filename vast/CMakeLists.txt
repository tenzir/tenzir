include_directories(${CMAKE_SOURCE_DIR}/libvast)
include_directories(${CMAKE_BINARY_DIR}/libvast)

add_executable(vast vast.cpp)
target_link_libraries(vast libvast ${CMAKE_THREAD_LIBS_INIT})
install(TARGETS vast DESTINATION bin)
