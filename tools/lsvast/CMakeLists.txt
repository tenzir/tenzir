option(VAST_ENABLE_LSVAST "Build the lsvast debugging utility" OFF)
add_feature_info("VAST_ENABLE_LSVAST" VAST_ENABLE_LSVAST
                 "build the lsvast debugging utility.")

if (NOT VAST_ENABLE_LSVAST)
  return()
endif ()

add_executable(lsvast lsvast.cpp)
target_link_libraries(lsvast PRIVATE vast::libvast vast::internal)
install(TARGETS lsvast DESTINATION "${CMAKE_INSTALL_BINDIR}")
