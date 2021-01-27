add_executable(flatcat flatcat.cpp)
target_link_libraries(flatcat PRIVATE flatbuffers)
install(TARGETS flatcat DESTINATION "${CMAKE_INSTALL_BINDIR}")
