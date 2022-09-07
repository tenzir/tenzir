option(VAST_ENABLE_LSVAST "Build the lsvast debugging utility" OFF)
add_feature_info("VAST_ENABLE_LSVAST" VAST_ENABLE_LSVAST
                 "build the lsvast debugging utility.")

if (NOT VAST_ENABLE_LSVAST)
  return()
endif ()

add_executable(
  lsvast
  src/lsvast.cpp
  src/print_index.cpp
  src/print_partition.cpp
  src/print_segment.cpp
  src/print_partition_synopsis.cpp
  src/print_synopsis.cpp
  src/print_segmented_file_header.cpp)
VASTTargetEnableTooling(lsvast)
target_link_libraries(lsvast PRIVATE vast::libvast vast::internal)
install(
  TARGETS lsvast
  DESTINATION "${CMAKE_INSTALL_BINDIR}"
  COMPONENT Runtime)

if (VAST_ENABLE_MANPAGES)
  add_custom_command(
    TARGET lsvast
    POST_BUILD
    COMMAND
      "${PANDOC}" -s -f markdown -t man
      "${CMAKE_CURRENT_SOURCE_DIR}/lsvast.1.md" -o
      "${CMAKE_CURRENT_BINARY_DIR}/lsvast.1"
    BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/lsvast.1"
    COMMENT "Generating lsvast.1"
    VERBATIM)
  install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/lsvast.1"
    DESTINATION "${CMAKE_INSTALL_MANDIR}/man1"
    COMPONENT Runtime)
endif ()
