option(VAST_ENABLE_LSVAST "Build the lsvast debugging utility" OFF)
add_feature_info("VAST_ENABLE_LSVAST" VAST_ENABLE_LSVAST
                 "build the lsvast debugging utility.")

if (NOT VAST_ENABLE_LSVAST)
  return()
endif ()

add_executable(lsvast src/lsvast.cpp src/print_index.cpp
                      src/print_partition.cpp src/print_segment.cpp)
VASTTargetEnableTooling(lsvast)
target_link_libraries(lsvast PRIVATE vast::libvast vast::internal)
install(TARGETS lsvast DESTINATION "${CMAKE_INSTALL_BINDIR}")

find_package(Pandoc)
if (PANDOC_FOUND)
  add_custom_target(
    lsvast-man ALL
    DEPENDS lsvast
    BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/lsvast.1"
    COMMAND
      pandoc -s -f markdown -t man "${CMAKE_CURRENT_SOURCE_DIR}/lsvast.1.md" -o
      "${CMAKE_CURRENT_BINARY_DIR}/lsvast.1"
    COMMENT "Generating man page lsvast.1"
    VERBATIM)
  install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/lsvast.1"
    DESTINATION "${CMAKE_INSTALL_MANDIR}/man1"
    OPTIONAL)
endif ()
