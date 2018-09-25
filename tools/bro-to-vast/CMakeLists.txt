include_directories(${CMAKE_SOURCE_DIR}/libvast)
include_directories(${CMAKE_BINARY_DIR}/libvast)

add_executable(bro-to-vast bro-to-vast.cpp)
target_link_libraries(bro-to-vast libvast ${BROKER_LIBRARY})
install(TARGETS bro-to-vast DESTINATION bin)
