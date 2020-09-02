add_executable(lsvast lsvast.cpp)
target_link_libraries(lsvast libvast)
install(TARGETS lsvast DESTINATION bin)
