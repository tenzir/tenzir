include_directories(${CMAKE_SOURCE_DIR}/libvast)
include_directories(${CMAKE_BINARY_DIR}/libvast)

add_executable(vast vast.cpp)
if (VAST_STATIC_EXECUTABLE)
  target_link_libraries(vast -static-libgcc -static-libstdc++ -static)
endif ()
target_link_libraries(vast libvast)
install(TARGETS vast DESTINATION bin)

# man pages
if (PANDOC_FOUND)
  set(man_page vast.1)
  set(man_generated "${CMAKE_CURRENT_BINARY_DIR}/${man_page}")
  file(
    WRITE ${CMAKE_CURRENT_BINARY_DIR}/make_man_${man_page}.cmake
    "execute_process(
      COMMAND ${CMAKE_BINARY_DIR}/bin/vast manual
      OUTPUT_FILE ${man_generated}.md)
    execute_process(
      COMMAND pandoc -s -f markdown -t man ${man_generated}.md
      OUTPUT_FILE ${man_generated})")
  add_custom_command(
    TARGET vast
    POST_BUILD
    BYPRODUCTS ${man_generated}
    COMMAND ${CMAKE_COMMAND} -P make_man_${man_page}.cmake
    COMMENT "Generating man page ${man_page}"
    VERBATIM)
  install(
    FILES "${man_generated}"
    DESTINATION "${CMAKE_INSTALL_MANDIR}/man1"
    OPTIONAL)
endif ()
