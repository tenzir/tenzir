add_executable(meta-index-dump meta_index_dump.cpp)
target_link_libraries(meta-index-dump PRIVATE vast::libvast vast::internal)
install(TARGETS meta-index-dump DESTINATION "${CMAKE_INSTALL_BINDIR}")
