include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../src)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src)

add_executable(dscat dscat.cc)
target_link_libraries(dscat libvast ${CAF_LIBRARIES})
install(TARGETS dscat DESTINATION bin)
