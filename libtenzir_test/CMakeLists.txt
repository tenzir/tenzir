if (NOT TENZIR_ENABLE_UNIT_TESTS)
  return()
endif ()

add_library(libtenzir_test STATIC src/actor_system.cpp src/symbols.cpp)
TenzirTargetEnableTooling(libtenzir_test)
target_include_directories(
  libtenzir_test PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
                        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_include_directories(libtenzir_test
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tenzir/test)
target_link_libraries(
  libtenzir_test
  PUBLIC CAF::test tenzir::libtenzir
  PRIVATE tenzir::internal)
set(isExe $<STREQUAL:$<TARGET_PROPERTY:TYPE>,EXECUTABLE>)
target_sources(
  libtenzir_test
  INTERFACE
    "$<${isExe}:$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp>>"
    "$<${isExe}:$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_DATADIR}/tenzir/test/main.cpp>>"
)
set_target_properties(
  libtenzir_test
  PROPERTIES SOVERSION "${TENZIR_VERSION_MAJOR}"
             VERSION "${TENZIR_VERSION_MAJOR}.${TENZIR_VERSION_MINOR}"
             OUTPUT_NAME tenzir_test
             EXPORT_NAME test)
add_library(tenzir::test ALIAS libtenzir_test)
install(
  TARGETS libtenzir_test
  EXPORT TenzirTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT Development)
install(
  FILES src/main.cpp
  DESTINATION "${CMAKE_INSTALL_DATADIR}/tenzir/test"
  COMPONENT Development)
install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/tenzir"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT Development)
