# utility for converting artifacts to source files via gen-vast-slices

set(generated_sources)

macro (log_to_slices format name)
  set(target_dir "${CMAKE_CURRENT_BINARY_DIR}/src/artifacts/logs/${format}")
  set(logfile
      "${CMAKE_CURRENT_SOURCE_DIR}/artifacts/logs/${format}/${name}.log")
  set(cppfile "${target_dir}/${name}.cpp")
  list(APPEND generated_sources "${cppfile}")
  set(cmd "$<TARGET_FILE:gen-vast-slices>")
  set(nn_arg "--namespace-name=artifacts::logs::${format}")
  set(vn_arg "--variable-name=${name}_buf")
  file(MAKE_DIRECTORY "${target_dir}")
  add_custom_command(
    OUTPUT "${cppfile}"
    COMMAND ${cmd} -i ${logfile} -o ${cppfile} -f ${format} ${nn_arg} ${vn_arg}
    DEPENDS gen-vast-slices "${logfile}")
endmacro ()

log_to_slices("zeek" "conn")

# test library definition

add_library(
  libvast_test STATIC
  "${generated_sources}"
  src/actor_system.cpp
  src/dummy_index.cpp
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
  PROPERTIES
    SOVERSION "${VERSION_YEAR}" VERSION "${VERSION_YEAR}.${VERSION_MONTH}"
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
