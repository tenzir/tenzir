option(VAST_ENABLE_VAST "Build the standalone vast binary"
       "${VAST_IS_NOT_SUBPROJECT}")
add_feature_info("VAST_ENABLE_VAST" VAST_ENABLE_VAST
                 "build the standalone vast binary.")

if (NOT VAST_ENABLE_VAST)
  return()
endif ()

add_executable(vast vast.cpp)
target_link_libraries(vast PRIVATE vast::internal vast::libvast)
install(TARGETS vast DESTINATION "${CMAKE_INSTALL_FULL_BINDIR}")

# -- init system integration ---------------------------------------------------

# TODO: This should be behind an option, and work for macOS and Linux as well.

# Install rc.d script on FreeBSD into PREFIX/etc/rc.d.
if (${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
  file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/scripts/freebsd-rc-vast"
       rc_template NEWLINE_CONSUME)
  string(REPLACE "%%PREFIX%%" ${CMAKE_INSTALL_PREFIX} rc_content ${rc_template})
  set(rc_file "${CMAKE_CURRENT_BINARY_DIR}/freebsd-rc-vast")
  file(WRITE ${rc_file} ${rc_content})
  install(
    FILES "${rc_file}"
    DESTINATION "${CMAKE_INSTALL_PREFIX}/etc/rc.d"
    RENAME "vast"
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_EXECUTE)
endif ()

# -- integration tests ---------------------------------------------------------

# TODO: Hide integration test suite behind an option that is enabled by default.

file(
  GENERATE
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/integration_$<CONFIG>.sh"
  CONTENT
    "#!/bin/sh
     if ! command -v jq >/dev/null 2>&1; then
       >&2 echo 'failed to find jq in $PATH'
       exit 1
     fi
     base_dir=\"${CMAKE_CURRENT_SOURCE_DIR}/integration\"
     env_dir=\"${CMAKE_CURRENT_BINARY_DIR}/integration_env\"
     app=\"$<TARGET_FILE:vast>\"
     set -e
     if [ ! -d \"$env_dir\" ]; then
       python3 -m venv \"$env_dir\"
     fi
     . \"$env_dir/bin/activate\"
     python -m pip install --upgrade pip
     python -m pip install -r \"$base_dir/requirements.txt\"
     $<$<BOOL:<${VAST_ENABLE_ARROW}>:python -m pip install pyarrow>
     python \"$base_dir/integration.py\" --app \"$app\" -d vast-integration-test \"$@\""
)

add_custom_target(
  integration
  COMMAND /bin/sh "${CMAKE_CURRENT_BINARY_DIR}/integration_$<CONFIG>.sh"
  USES_TERMINAL)

install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/vast/integration"
  DESTINATION "${CMAKE_INSTALL_FULL_DATADIR}/vast"
  PATTERN "integration/vast.yaml.example" EXCLUDE)

install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/vast.yaml.example"
        DESTINATION "${CMAKE_INSTALL_FULL_DATADIR}/vast/integration")

# -- man page ------------------------------------------------------------------

# TODO: Hide behind a feature flag that is enabled by default and require
# Pandoc to be available.
# TODO: Keep a copy of the man page in the repository such that we know the last
# edit date. If pandoc+git is available, rebuild the man page.

find_package(Pandoc)
if (PANDOC_FOUND)
  set(man_page vast.1)
  set(man_generated "${CMAKE_CURRENT_BINARY_DIR}/${man_page}")
  file(
    WRITE ${CMAKE_CURRENT_BINARY_DIR}/make_man_${man_page}.cmake
    "execute_process(
      COMMAND ${CMAKE_BINARY_DIR}/bin/vast manual
      OUTPUT_FILE ${man_generated}.md)
    execute_process(
      COMMAND pandoc -s -f markdown -t man ${man_generated}.md
      OUTPUT_FILE ${man_generated})")
  add_custom_command(
    TARGET vast
    POST_BUILD
    BYPRODUCTS ${man_generated}
    COMMAND ${CMAKE_COMMAND} -P make_man_${man_page}.cmake
    COMMENT "Generating man page ${man_page}"
    VERBATIM)
  install(
    FILES "${man_generated}"
    DESTINATION "${CMAKE_INSTALL_MANDIR}/man1"
    OPTIONAL)
endif ()
