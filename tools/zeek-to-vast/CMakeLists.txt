add_executable(zeek-to-vast zeek-to-vast.cpp)
target_link_libraries(zeek-to-vast libvast zeek::broker caf::core)
multi_config_target_symlink(zeek-to-vast)
install(TARGETS zeek-to-vast DESTINATION bin)
