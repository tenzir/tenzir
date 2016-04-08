include_directories(${CMAKE_SOURCE_DIR}/libvast)
include_directories(${CMAKE_BINARY_DIR}/libvast)

add_executable(dscat dscat.cpp)
target_link_libraries(dscat libvast ${CAF_LIBRARIES})
install(TARGETS dscat DESTINATION bin)
