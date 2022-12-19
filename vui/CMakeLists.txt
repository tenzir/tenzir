option(VAST_ENABLE_VUI "Build the VAST terminal UI" OFF)
add_feature_info("VAST_ENABLE_VUI" VAST_ENABLE_VUI
                 "build the VAST terminal UI.")

if (NOT VAST_ENABLE_VUI)
  return()
endif ()

add_executable(
  vui
  src/vui.cpp)
VASTTargetEnableTooling(vui)
target_link_libraries(vui PRIVATE vast::libvast vast::internal)
install(
  TARGETS vui
  DESTINATION "${CMAKE_INSTALL_BINDIR}"
  COMPONENT Runtime)
