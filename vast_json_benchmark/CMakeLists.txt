# option(VAST_ENABLE_VAST "Build the standalone vast binary"
#        "${VAST_IS_NOT_SUBPROJECT}")
# add_feature_info("VAST_ENABLE_VAST" VAST_ENABLE_VAST
#                  "build the standalone vast binary.")

# if (NOT VAST_ENABLE_VAST)
#   return()
# endif ()

add_executable(vast_json_benchmark vast_json_benchmark.cpp)
target_link_libraries(vast_json_benchmark PRIVATE vast::internal vast::libvast)
install(TARGETS vast_json_benchmark DESTINATION "${CMAKE_INSTALL_BINDIR}")
