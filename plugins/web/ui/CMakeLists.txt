# The install dir of the UI root relative to the install prefix of VAST itself.
set(ui_install_dir "${CMAKE_INSTALL_DATADIR}/vast/plugin/web/www")

set(VAST_WEB_UI_BUNDLE
    ""
    CACHE STRING "Specify a path to an external web UI bundle")
if (VAST_WEB_UI_BUNDLE)
  file(REMOVE_RECURSE "${CMAKE_BINARY_DIR}/${ui_install_dir}")
  file(COPY "${VAST_WEB_UI_BUNDLE}"
       DESTINATION "${CMAKE_BINARY_DIR}/${ui_install_dir}")
else ()
  # Build and install the frontend as a static site.
  find_program(PNPM_PATH pnpm)
  if (NOT PNPM_PATH)
    message(
      FATAL_ERROR
        "Cannot find 'pnpm' in PATH, which is required for building the web frontend"
    )
  endif ()

  # We reconfigure whenever any file in the current directory changes, but exclude
  # files that match the exclude pattern here. Ideally we'd want to use the
  # existing .gitignore file for that, but then we'd need another indirection in
  # CMake, and potential triple escaping is really hard to maintain.
  file(
    GLOB_RECURSE ui_sources
    RELATIVE "${CMAKE_CURRENT_LIST_DIR}"
    CONFIGURE_DEPENDS *)
  set(sources_exclude_pattern "^(.svelte-kit|build|node_modules)")
  list(FILTER ui_sources EXCLUDE REGEX "${sources_exclude_pattern}/")

  # A short-lived CMake script that copies over the UI sources to the build
  # directory for building while respecting the exclude patterns. In theory, we
  # can also use @ONLY rather than COPYONLY here to pass in CMake options to the
  # UI sources.
  file(
    WRITE "${CMAKE_CURRENT_BINARY_DIR}/copy-ui.cmake"
    "\
    file(
      GLOB old_ui_sources
      LIST_DIRECTORIES ON
      RELATIVE \"${CMAKE_CURRENT_BINARY_DIR}\"
      \"${CMAKE_CURRENT_BINARY_DIR}/*\")
    list(FILTER old_ui_sources EXCLUDE REGEX \"${sources_exclude_pattern}\")
    if (old_ui_sources)
      file(REMOVE \${old_ui_sources})
    endif ()
    list(APPEND ui_sources ${ui_sources})
    foreach (ui_source IN LISTS ui_sources)
      configure_file(
        \"${CMAKE_CURRENT_LIST_DIR}/\${ui_source}\"
        \"${CMAKE_CURRENT_BINARY_DIR}/\${ui_source}\"
        COPYONLY)
    endforeach ()")

  add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/ui.marker"
    BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/copy-ui.cmake"
    COMMENT "Builind UI to ${CMAKE_BINARY_DIR}/${ui_install_dir}"
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
    COMMAND "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_BINARY_DIR}/copy-ui.cmake"
    COMMAND "${PNPM_PATH}" install
    COMMAND "${PNPM_PATH}" build
    COMMAND "${CMAKE_COMMAND}" -E rm -rf "${CMAKE_BINARY_DIR}/${ui_install_dir}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory
            "${CMAKE_BINARY_DIR}/${ui_install_dir}"
    COMMAND
      "${CMAKE_COMMAND}" -E copy_directory "${CMAKE_CURRENT_BINARY_DIR}/build"
      "${CMAKE_BINARY_DIR}/${ui_install_dir}/"
    COMMAND "${CMAKE_COMMAND}" -E touch "${CMAKE_CURRENT_BINARY_DIR}/ui.marker"
    DEPENDS ${ui_sources})

  add_custom_target(web-ui DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/ui.marker")
  add_dependencies(web web-ui)
endif ()

install(
  DIRECTORY "${CMAKE_BINARY_DIR}/${ui_install_dir}"
  COMPONENT Runtime
  DESTINATION "${ui_install_dir}")
