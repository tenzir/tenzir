add_library(
  libvast_test STATIC
  src/actor_system.cpp
  src/events.cpp
  src/node.cpp
  src/symbols.cpp
  src/table_slices.cpp)
target_compile_definitions(
  libvast_test
  PUBLIC
    $<BUILD_INTERFACE:VAST_TEST_PATH=\"${CMAKE_CURRENT_SOURCE_DIR}/\">
    $<INSTALL_INTERFACE:VAST_TEST_PATH=\"\${_IMPORT_PREFIX}/${CMAKE_INSTALL_DATADIR}/vast/test/\">
)
target_include_directories(
  libvast_test PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
                      $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_include_directories(libvast_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/vast/test)
target_link_libraries(libvast_test PUBLIC caf::test libvast)
set(isExe $<STREQUAL:$<TARGET_PROPERTY:TYPE>,EXECUTABLE>)
target_sources(
  libvast_test
  INTERFACE
    "$<${isExe}:$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp>>"
    "$<${isExe}:$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_DATADIR}/vast/test/main.cpp>>"
)
set_target_properties(
  libvast_test
  PROPERTIES SOVERSION "${VERSION_YEAR}" VERSION
                                         "${VERSION_YEAR}.${VERSION_MONTH}"
             OUTPUT_NAME vast_test EXPORT_NAME test)
add_library(vast::test ALIAS libvast_test)
install(
  TARGETS libvast_test
  EXPORT VASTTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES src/main.cpp DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/test")
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/vast"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/artifacts"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/test")
