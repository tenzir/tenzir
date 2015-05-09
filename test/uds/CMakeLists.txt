include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../src)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src)

add_executable(uds_server server.cc)
target_link_libraries(uds_server libvast)
install(TARGETS uds_server DESTINATION bin)

add_executable(uds_client client.cc)
target_link_libraries(uds_client libvast)
install(TARGETS uds_client DESTINATION bin)
