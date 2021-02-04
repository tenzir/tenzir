# Create a library target for our plugin.
add_library(example example.cpp)

# Link against both libvast and against libvast_internal. The latter is
# optional, but recommended to enable a better development experience.
target_link_libraries(
  example
  PUBLIC vast::libvast
  PRIVATE vast::internal)

# Set the library output directory to <libdir>/vast/plugins. This is necessary
# so we can run the integration tests with plugins.
set_target_properties(
  example PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                     "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR}/vast/plugins")

# Install the plugin library to <libdir>/vast/plugins.
install(TARGETS example DESTINATION "${CMAKE_INSTALL_LIBDIR}/vast/plugins")

# Install the bundled schema files to <datadir>/vast.
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/schema"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/vast")

# Add unit tests for the example plugin.
add_subdirectory(tests)
