# Bail out early if we're asked not to build unit tests.
if (NOT VAST_ENABLE_UNIT_TESTS)
  return()
endif ()

# Create a custom test executable.
add_executable(example-tests tests.cpp)

# Link against our example plugin, the testing framework, and against
# libvast_internal.
target_link_libraries(example-tests PRIVATE example vast::test vast::internal)

# Register the test for the test build target. If you put something else besides
# a target name after COMMAND, it will register as a command line to run.
add_test(NAME example COMMAND example-tests)
