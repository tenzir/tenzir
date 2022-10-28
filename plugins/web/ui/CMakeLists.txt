# Build and install the frontend as a static site.

file(
  GLOB_RECURSE ui_sources
  RELATIVE "${CMAKE_CURRENT_LIST_DIR}"
  CONFIGURE_DEPENDS *)

find_program(PNPM_PATH pnpm)
if (NOT PNPM_PATH)
  message(
    FATAL_ERROR
      "Cannot find 'pnpm' in PATH, which is required for building the web frontend"
  )
endif ()

set(www_dir "${VAST_CMAKE_INSTALL_DATADIR}/vast/plugin/web/www")

set(ui_sources_absolute ${ui_sources})
list(TRANSFORM ui_sources_absolute PREPEND "${CMAKE_CURRENT_LIST_DIR}/")

add_custom_command(
  OUTPUT "${PROJECT_BINARY_DIR}/${www_dir}"
  WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
  COMMAND "${CMAKE_COMMAND}" -E rm -f ${ui_sources}
  COMMAND "${CMAKE_COMMAND}" -E copy ${ui_sources_absolute}
          "${CMAKE_CURRENT_BINARY_DIR}"
  COMMAND "${PNPM_PATH}" install
  COMMAND "${PNPM_PATH}" build
  COMMAND "${CMAKE_COMMAND}" -E copy_directory
          "${CMAKE_CURRENT_BINARY_DIR}/build" "${PROJECT_BINARY_DIR}/${www_dir}"
  DEPENDS ${ui_sources})

add_custom_target(web-ui DEPENDS "${PROJECT_BINARY_DIR}/${www_dir}")

install(
  DIRECTORY "${PROJECT_BINARY_DIR}/${www_dir}"
  COMPONENT Runtime
  DESTINATION "${www_dir}")
