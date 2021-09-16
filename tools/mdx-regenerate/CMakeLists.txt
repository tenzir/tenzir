option(VAST_ENABLE_MDX_REGENERATE "Build the mdx-regenerate debugging utility"
       OFF)
add_feature_info("VAST_ENABLE_MDX_REGENERATE" VAST_ENABLE_MDX_REGENERATE
                 "build the mdx-regenerate utility.")

if (NOT VAST_ENABLE_MDX_REGENERATE)
  return()
endif ()

add_executable(mdx-regenerate main.cpp)
VASTTargetEnableTooling(mdx-regenerate)

target_link_libraries(mdx-regenerate PRIVATE vast::libvast vast::internal)

install(TARGETS mdx-regenerate DESTINATION "${CMAKE_INSTALL_BINDIR}")
