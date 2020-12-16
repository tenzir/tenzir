# Find modules are needed by the consumer in case of a static build, or if the
# linkage is PUBLIC or INTERFACE.
macro (provide_find_module name)
  if (NOT BUILD_SHARED_LIBS)
    message(STATUS "Providing cmake module for ${name}")
    configure_file("${PROJECT_SOURCE_DIR}/cmake/Find${name}.cmake" ${CMAKE_BINARY_DIR} COPYONLY)
    install(
      FILES "${CMAKE_BINARY_DIR}/Find${name}.cmake"
      DESTINATION ${INSTALL_VAST_CMAKEDIR}
      COMPONENT dev)
  endif ()
endmacro ()
