add_library(example example.cpp)
target_link_libraries(example libvast)
install(TARGETS example DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}/vast/plugins)
