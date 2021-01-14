# Create a library target for our plugin.
add_library(example example.cpp)

# Link against both libvast and against libvast_internal. The latter is
# optional, but recommended to enable a better development experience.
target_link_libraries(example PRIVATE vast::libvast vast::internal)

# Install the plugin library to <libdir>/vast/plugins.
install(TARGETS example DESTINATION "${CMAKE_INSTALL_LIBDIR}/vast/plugins")
