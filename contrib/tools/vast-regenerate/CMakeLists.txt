option(VAST_ENABLE_VAST_REGENERATE
       "Build the vast-regenerate debugging utility" OFF)
add_feature_info("VAST_ENABLE_VAST_REGENERATE" VAST_ENABLE_VAST_REGENERATE
                 "build the vast-regenerate utility.")

if (NOT VAST_ENABLE_VAST_REGENERATE)
  return()
endif ()

add_executable(vast-regenerate main.cpp)
VASTTargetEnableTooling(vast-regenerate)

target_link_libraries(vast-regenerate PRIVATE vast::libvast vast::internal)

install(
  TARGETS vast-regenerate
  DESTINATION "${CMAKE_INSTALL_BINDIR}"
  COMPONENT Runtime)
