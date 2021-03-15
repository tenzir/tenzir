include_guard(GLOBAL)

function (VASTRegisterPlugin)
  include(GNUInstallDirs)

  cmake_parse_arguments(PLUGIN "" "TARGET" "SOURCES;TEST_SOURCES" ${ARGN})

  macro (target_link_whole_archive target visibility library)
    get_target_property(target_type ${library} TYPE)
    if (target_type STREQUAL "STATIC_LIBRARY")
      # Prevent elision of self-registration code in statically linked libraries,
      # c.f., https://www.bfilipek.com/2018/02/static-vars-static-lib.html
      target_link_options(
        ${target}
        ${visibility}
        $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>>:LINKER:-force_load,$<TARGET_FILE:${library}>>
        $<$<CXX_COMPILER_ID:GNU>:LINKER:--whole-archive,$<TARGET_FILE:${library}>,--no-whole-archive>
        $<$<CXX_COMPILER_ID:MSVC>:LINKER:/WHOLEARCHIVE,$<TARGET_FILE:${library}>>
      )
    endif ()
    target_link_libraries(${target} ${visibility} ${library})
  endmacro ()

  if (NOT PLUGIN_TARGET)
    message(
      FATAL_ERROR "TARGET must be specified in call to VASTRegisterPlugin")
  endif ()

  if (NOT PLUGIN_SOURCES)
    message(
      FATAL_ERROR "SOURCES must be specified in call to VASTRegisterPlugin")
  endif ()

  if (VAST_ENABLE_STATIC_PLUGINS)
    # Create a static library target for our plugin.
    add_library(${PLUGIN_TARGET} STATIC ${PLUGIN_SOURCES})

    # Use static versions of VAST_REGISTER_PLUGIN family of macros.
    target_compile_definitions(${PLUGIN_TARGET}
                               PRIVATE VAST_ENABLE_STATIC_PLUGINS_INTERNAL)

    # Link our static library against the vast binary directly.
    target_link_whole_archive(vast PRIVATE ${PLUGIN_TARGET})
  else ()
    # Create a shared library target for our plugin.
    add_library(${PLUGIN_TARGET} SHARED ${PLUGIN_SOURCES})
  endif ()

  # Install the bundled schema files to <datadir>/vast.
  if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/schema")
    install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/schema"
            DESTINATION "${CMAKE_INSTALL_DATADIR}/vast")
  endif ()

  # Install the plugin library to <libdir>/vast/plugins, and also configure the
  # library output directory accordingly.
  install(TARGETS ${PLUGIN_TARGET}
          DESTINATION "${CMAKE_INSTALL_LIBDIR}/vast/plugins")
  set_target_properties(
    ${PLUGIN_TARGET}
    PROPERTIES LIBRARY_OUTPUT_DIRECTORY
               "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR}/vast/plugins")

  # Implicitly link plugins against vast::libvast and vast::internal:
  target_link_libraries(
    ${PLUGIN_TARGET}
    PUBLIC vast::libvast
    PRIVATE vast::internal)

  # Ensure the man-page target is generated after all plugins.
  if (TARGET vast-man)
    add_dependencies(vast-man ${PLUGIN_TARGET})
  endif ()

  # Setup unit tests.
  if (VAST_ENABLE_UNIT_TESTS AND PLUGIN_TEST_SOURCES)
    add_executable(${PLUGIN_TARGET}-test ${PLUGIN_TEST_SOURCES})
    target_link_libraries(${PLUGIN_TARGET}-test PRIVATE vast::test
                                                        vast::internal)
    target_link_whole_archive(${PLUGIN_TARGET}-test PRIVATE ${PLUGIN_TARGET})
    add_test(NAME ${PLUGIN_TARGET} COMMAND ${PLUGIN_TARGET}-test)
  endif ()
endfunction ()
