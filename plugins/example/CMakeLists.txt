option(VAST_ENABLE_EXAMPLE_PLUGIN "Build example plugin" OFF)
add_feature_info("VAST_ENABLE_EXAMPLE_PLUGIN" VAST_ENABLE_EXAMPLE_PLUGIN
                 "build example plugin.")

if (NOT VAST_ENABLE_EXAMPLE_PLUGIN)
  return()
endif ()

add_library(example example.cpp)
target_link_libraries(example PRIVATE vast::libvast vast::internal)
install(TARGETS example DESTINATION "${CMAKE_INSTALL_LIBDIR}/vast/plugins")
