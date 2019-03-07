add_executable(gen-vast-slices gen-vast-slices.cpp)
target_link_libraries(gen-vast-slices libvast)
install(TARGETS gen-vast-slices DESTINATION bin)
