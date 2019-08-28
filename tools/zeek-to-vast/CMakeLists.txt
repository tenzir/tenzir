add_executable(zeek-to-vast zeek-to-vast.cpp)
target_link_libraries(zeek-to-vast libvast zeek::broker caf::core)
install(TARGETS zeek-to-vast DESTINATION bin)
