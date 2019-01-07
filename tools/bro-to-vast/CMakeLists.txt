add_executable(bro-to-vast bro-to-vast.cpp)
target_link_libraries(bro-to-vast libvast broker::broker)
install(TARGETS bro-to-vast DESTINATION bin)
