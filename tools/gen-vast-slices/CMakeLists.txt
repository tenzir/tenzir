add_executable(gen-vast-slices gen-vast-slices.cpp)
target_link_libraries(gen-vast-slices libvast)
multi_config_target_symlink(gen-vast-slices)
install(TARGETS gen-vast-slices DESTINATION bin)
