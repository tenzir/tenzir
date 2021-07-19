option(VAST_ENABLE_DSCAT "Build the dscat utility" OFF)
add_feature_info("VAST_ENABLE_DSCAT" VAST_ENABLE_DSCAT
                 "build the dscat utility.")

if (NOT VAST_ENABLE_DSCAT)
  return()
endif ()

add_executable(dscat dscat.cpp)
VASTTargetEnableTooling(dscat)
target_link_libraries(dscat PRIVATE vast::libvast vast::internal CAF::core)
install(TARGETS dscat DESTINATION "${CMAKE_INSTALL_BINDIR}")
