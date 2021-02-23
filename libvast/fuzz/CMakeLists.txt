add_executable(fuzz-export fuzz_export.cpp)
target_link_libraries(fuzz-export PRIVATE vast::libvast vast::internal)
install(TARGETS fuzz-export DESTINATION "${CMAKE_INSTALL_BINDIR}")