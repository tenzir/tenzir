include_directories(${CMAKE_SOURCE_DIR}/libvast)
include_directories(${CMAKE_BINARY_DIR}/libvast)

add_executable(vast vast.cpp)
if (VAST_STATIC_EXECUTABLE)
  target_link_libraries(vast -static-libgcc -static-libstdc++ -static)
endif ()
target_link_libraries(vast libvast)
install(TARGETS vast DESTINATION bin)
