option(VAST_ENABLE_VAST "Build the standalone vast binary"
       "${VAST_IS_NOT_SUBPROJECT}")
add_feature_info("VAST_ENABLE_VAST" VAST_ENABLE_VAST
                 "build the standalone vast binary.")

if (NOT VAST_ENABLE_VAST)
  return()
endif ()

add_executable(vast vast.cpp)
target_link_libraries(vast PRIVATE vast::internal vast::libvast)
install(TARGETS vast DESTINATION "${CMAKE_INSTALL_BINDIR}")

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
     python \"$base_dir/integration.py\" \
      --app \"$app\" \
      --directory vast-integration-test \
      \"$@\"
      . \"${CMAKE_CURRENT_BINARY_DIR}/plugin_integration.sh\"")

add_custom_target(
  integration
  COMMAND /bin/sh "${CMAKE_CURRENT_BINARY_DIR}/integration_$<CONFIG>.sh" "-v"
          "DEBUG"
  USES_TERMINAL)

add_custom_target(
  link-integration-directory ALL
  COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_BINARY_DIR}/share/vast"
  COMMAND
    ${CMAKE_COMMAND} -E create_symlink "${CMAKE_CURRENT_SOURCE_DIR}/integration"
    "${CMAKE_BINARY_DIR}/share/vast/integration"
  COMMENT "Linking integration test directory")

install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/integration"
  DESTINATION "${CMAKE_INSTALL_DATADIR}/vast"
  PATTERN "integration/vast.yaml.example" EXCLUDE)

install(FILES "${PROJECT_SOURCE_DIR}/vast.yaml.example"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/integration")

# -- other tests ---------------------------------------------------------------

add_custom_target(
  stress
  COMMAND ${CMAKE_COMMAND} -E env "VAST=$<TARGET_FILE:vast>" "${CMAKE_CURRENT_SOURCE_DIR}/test/stress.sh"
  USES_TERMINAL)

# -- add plugins ---------------------------------------------------------------

option(VAST_PLUGINS "Specify a list of plugins to build with VAST" "")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/plugin_integration.sh" "")
if (VAST_PLUGINS)
  foreach (plugin_source_dir IN LISTS VAST_PLUGINS)
    if (NOT IS_ABSOLUTE "${plugin_source_dir}"
        OR NOT EXISTS "${plugin_source_dir}/CMakeLists.txt")
      message(
        FATAL_ERROR
          "Specified plugin \"${plugin_source_dir}\" must be an absolute path to an existing directory that contains a CMakeLists.txt file."
      )
    endif ()
    # Note that we must explicitly specify a binary directory, because the plugin
    # source directory may not be a subdirectory of the current list directory.
    string(MD5 plugin_path_hash "${plugin_source_dir}")
    string(SUBSTRING "${plugin_path_hash}" 0 8 plugin_path_hash)
    get_filename_component(plugin_name "${plugin_source_dir}" NAME)
    string(TOLOWER "${plugin_name}" plugin_name)
    list(APPEND bundled_plugins "${plugin_name}")
    set(plugin_binary_dir "plugin-${plugin_name}-${plugin_path_hash}")
    add_subdirectory("${plugin_source_dir}" "${plugin_binary_dir}")
    if (EXISTS "${plugin_source_dir}/integration/tests.yaml")
      file(
        APPEND "${CMAKE_CURRENT_BINARY_DIR}/plugin_integration.sh"
        "python \"\$base_dir/integration.py\" \
        --app \"\$app\" \
        --set \"${plugin_source_dir}/integration/tests.yaml\" \
        --directory \"vast-integration-test-${plugin_binary_dir}\" \
        \"\$@\"\n")
    endif ()
    # Install custom schemas
    if (TARGET vast-schema AND EXISTS "${plugin_source_dir}/schema")
      file(
        GLOB_RECURSE
        plugin_schema_files
        CONFIGURE_DEPENDS
        "${plugin_source_dir}/schema/*.schema"
        "${plugin_source_dir}/schema/*.yml"
        "${plugin_source_dir}/schema/*.yaml")
      foreach (plugin_schema_file IN LISTS plugin_schema_files)
        string(REGEX REPLACE "^${plugin_source_dir}/schema/" ""
                             relative_plugin_schema_file ${plugin_schema_file})
        string(MD5 plugin_schema_file_hash "${plugin_schema_file}")
        add_custom_target(
          vast-schema-${plugin_path_hash}-${plugin_schema_file_hash} ALL
          BYPRODUCTS
            "${CMAKE_BINARY_DIR}/share/vast/schema/${relative_plugin_schema_file}"
          DEPENDS vast-schema
          COMMAND
            ${CMAKE_COMMAND} -E copy "${plugin_schema_file}"
            "${CMAKE_BINARY_DIR}/share/vast/schema/${relative_plugin_schema_file}"
          COMMENT
            "Copying schema file ${relative_plugin_schema_file} for plugin ${plugin_name}"
        )
      endforeach ()
    endif ()
  endforeach ()
endif ()

option(VAST_ENABLE_PLUGIN_AUTOLOADING "Always load all bundled plugins" OFF)
add_feature_info(
  "VAST_ENABLE_PLUGIN_AUTOLOADING" VAST_ENABLE_PLUGIN_AUTOLOADING
  "always load all bundled plugins.")
if (VAST_ENABLE_PLUGIN_AUTOLOADING AND bundled_plugins)
  list(TRANSFORM bundled_plugins PREPEND "\"lib")
  list(TRANSFORM bundled_plugins APPEND "\"")
  list(JOIN bundled_plugins ", " joined_bundled_plugins)
  message(WARNING "bundled plugins: ${bundled_plugins}")
  message(WARNING "joined bundled plugins: ${joined_bundled_plugins}")
  target_compile_definitions(
    vast PRIVATE "VAST_ENABLED_PLUGINS=${joined_bundled_plugins}")
endif ()

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
