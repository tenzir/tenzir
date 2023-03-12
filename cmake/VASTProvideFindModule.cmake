set(_vast_find_module_dir "${CMAKE_CURRENT_LIST_DIR}")
# Find modules are needed by the consumer in case of a static build, or if the
# linkage is PUBLIC or INTERFACE.
macro (provide_find_module name)
  message(VERBOSE "Providing cmake module for ${name}")
  configure_file("${_vast_find_module_dir}/Find${name}.cmake"
                 ${CMAKE_BINARY_DIR} COPYONLY)
  install(
    FILES "${CMAKE_BINARY_DIR}/Find${name}.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/vast"
    COMPONENT Development)
endmacro ()
