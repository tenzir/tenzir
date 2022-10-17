# Build and install the frontend as a static site.

file(GLOB_RECURSE ui-sources *.svelte *.ts)

#find_program(pnpm)

set(www_dir "${VAST_CMAKE_INSTALL_DATADIR}/vast/plugin/web/www")

add_custom_command(
  OUTPUT "${PROJECT_BINARY_DIR}/${www_dir}"
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMAND pnpm install
  COMMAND pnpm build
  COMMAND
    "${CMAKE_COMMAND}" -E copy "${CMAKE_CURRENT_SOURCE_DIR}/build" "${PROJECT_BINARY_DIR}/${www_dir}"
  DEPENDS ${ui-sources})

add_custom_target(web-ui DEPENDS "${PROJECT_BINARY_DIR}/${www_dir}")

install(DIRECTORY "${PROJECT_BINARY_DIR}/${www_dir}"
        DESTINATION "${www_dir}")
