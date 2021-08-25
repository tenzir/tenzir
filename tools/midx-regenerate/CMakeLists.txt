option(VAST_ENABLE_MIDX_REGENERATE "Build the midx-regenerate debugging utility" OFF)
add_feature_info("VAST_ENABLE_MIDX_REGENERATE" VAST_ENABLE_MIDX_REGENERATE
                 "build the midx-regenerate debugging utility.")

if (NOT VAST_ENABLE_MIDX_REGENERATE)
  return()
endif ()

add_executable(midx-regenerate main.cpp)
VASTTargetEnableTooling(midx-regenerate)

target_link_libraries(midx-regenerate PRIVATE vast::libvast vast::internal)

find_package(fmt 5.2.1 REQUIRED)
target_link_libraries(midx-regenerate PUBLIC fmt::fmt)

install(TARGETS midx-regenerate DESTINATION "${CMAKE_INSTALL_BINDIR}")
