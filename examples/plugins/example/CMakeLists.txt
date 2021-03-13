set(entrypoint example.cpp)
set(sources ${entrypoint})

if (VAST_ENABLE_STATIC_PLUGINS)

  # Building a plugin as a static library cannot work with the dynamic loading
  # mechanism by design. The best we can do is to build the sources directly
  # with VAST. This comes with a few disadvantages:
  # - Plugins cannot be selectively enabled or disabled.
  # - Plugins must allocate memory before we enter main, which means we must
  #   rely on the the plugin constructor not to throw.
  # - Unit tests do not function as there exists no library to test separately.
  target_sources(vast PRIVATE ${sources})

  if (VAST_ENABLE_STATIC_EXECUTABLE)
    # Prevent elision of self-registration code in statically linked binaries,
    # c.f., https://www.bfilipek.com/2018/02/static-vars-static-lib.html
    set_source_files_properties(${entrypoint} PROPERTIES COMPILE_FLAGS
      -Wl,--whole-archive)
  endif ()

else ()

  # Create a library target for our plugin.
  add_library(example ${sources})

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

  # Add unit tests for the example plugin.
  add_subdirectory(tests)

endif ()

# Install the bundled schema files to <datadir>/vast.
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/schema"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/vast")
