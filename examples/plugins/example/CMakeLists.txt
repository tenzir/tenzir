set(plugin_sources example.cpp)
set(tests_sources tests/example.cpp)

macro (target_link_whole_archive target visibility library)
  get_target_property(target_type ${library} TYPE)
  if (target_type STREQUAL "STATIC_LIBRARY")
    # Prevent elision of self-registration code in statically linked libraries,
    # c.f., https://www.bfilipek.com/2018/02/static-vars-static-lib.html
    target_link_options(${target} ${visibility}
      $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>>:LINKER:-force_load,$<TARGET_FILE:${library}>>
      $<$<CXX_COMPILER_ID:GNU>:LINKER:--whole-archive,$<TARGET_FILE:${library}>,--no-whole-archive>
      $<$<CXX_COMPILER_ID:MSVC>:LINKER:/WHOLEARCHIVE,$<TARGET_FILE:${library}>>)
  endif ()
  target_link_libraries(${target} ${visibility} ${library})
endmacro ()

if (VAST_ENABLE_STATIC_PLUGINS)
  # Create a static library target for our plugin.
  add_library(example STATIC ${plugin_sources})

  # Use static versions of VAST_REGISTER_PLUGIN family of macros.
  target_compile_definitions(example PRIVATE VAST_ENABLE_STATIC_PLUGINS_INTERNAL)

  # Link our static library against the vast binary directly.
  target_link_whole_archive(vast PRIVATE example)
else ()
  # Create a shared library target for our plugin.
  add_library(example SHARED ${plugin_sources})
endif ()

# Install the bundled schema files to <datadir>/vast.
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/schema"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/vast")

# Set the library output directory to <libdir>/vast/plugins. This is necessary
# so we can run the integration tests with plugins.
set_target_properties(
  example PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                    "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR}/vast/plugins")

# Install the plugin library to <libdir>/vast/plugins.
install(TARGETS example DESTINATION "${CMAKE_INSTALL_LIBDIR}/vast/plugins")

# Ensure the man-page target is generated after all plugins.
if (TARGET vast-man)
  add_dependencies(vast-man example)
endif ()

# Link against both libvast and against libvast_internal. The latter is
# optional, but recommended to enable a better development experience.
target_link_libraries(
  example
  PUBLIC vast::libvast
  PRIVATE vast::internal)

if (VAST_ENABLE_UNIT_TESTS)
  # Create a custom test executable.
  add_executable(example-tests ${tests_sources})

  # Link against our the testing framework, and against libvast_internal.
  target_link_libraries(example-tests PRIVATE vast::test vast::internal)

  # Explicitly link against our example plugin.
  target_link_whole_archive(vast PRIVATE example)

  # Register the test for the test build target. If you put something else besides
  # a target name after COMMAND, it will register as a command line to run.
  add_test(NAME example COMMAND example-tests)
endif ()
