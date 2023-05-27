if (NOT VAST_ENABLE_UNIT_TESTS)
  return()
endif ()

add_library(
  libvast_test STATIC src/actor_system.cpp src/events.cpp src/node.cpp
                      src/symbols.cpp src/table_slices.cpp)
VASTTargetEnableTooling(libvast_test)
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
target_link_libraries(
  libvast_test
  PUBLIC CAF::test vast::libvast
  PRIVATE vast::internal)
set(isExe $<STREQUAL:$<TARGET_PROPERTY:TYPE>,EXECUTABLE>)
target_sources(
  libvast_test
  INTERFACE
    "$<${isExe}:$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp>>"
    "$<${isExe}:$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_DATADIR}/vast/test/main.cpp>>"
)
set_target_properties(
  libvast_test
  PROPERTIES SOVERSION "${VAST_VERSION_MAJOR}"
             VERSION "${VAST_VERSION_MAJOR}.${VAST_VERSION_MINOR}"
             OUTPUT_NAME vast_test
             EXPORT_NAME test)
add_library(vast::test ALIAS libvast_test)
install(
  TARGETS libvast_test
  EXPORT libVASTTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT Development)
install(
  FILES src/main.cpp
  DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/test"
  COMPONENT Development)
install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/vast"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT Development)
install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/artifacts"
  DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/test"
  COMPONENT Development)
