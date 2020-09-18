add_executable(lsvast lsvast.cpp)
target_link_libraries(lsvast libvast ${flatbuffers_target})
install(TARGETS lsvast DESTINATION bin)
