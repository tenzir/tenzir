option(VAST_ENABLE_VAST "Build the standalone vast binary"
       "${VAST_IS_NOT_SUBPROJECT}")
add_feature_info("VAST_ENABLE_VAST" VAST_ENABLE_VAST
                 "build the standalone vast binary.")

if (NOT VAST_ENABLE_VAST)
  return()
endif ()

add_executable(vast vast.cpp)
VASTTargetEnableTooling(vast)
target_link_libraries(vast PRIVATE vast::internal vast::libvast)
VASTTargetLinkWholeArchive(vast PRIVATE vast::native_plugins)
install(TARGETS vast DESTINATION "${CMAKE_INSTALL_BINDIR}")
add_executable(vast::vast ALIAS vast)

# Install vast in PREFIX/lib and headers in PREFIX/include/vast.
install(
  TARGETS vast
  EXPORT VASTTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

# -- example configuration file -----------------------------------------------

file(
  WRITE "${CMAKE_CURRENT_BINARY_DIR}/example-config.cmake"
  "\
  cmake_minimum_required(VERSION 3.15...3.20 FATAL_ERROR)
  file(READ \"${PROJECT_SOURCE_DIR}/vast.yaml.example\" content)
  # Randomly generated string that temporarily replaces semicolons.
  set(dummy \"J.3t26kvfjEoi9BXbf2j.qMY\")
  string(REPLACE \";\" \"\${dummy}\" content \"\${content}\")
  string(REPLACE \"\\n\" \";\" content \"\${content}\")
  list(TRANSFORM content PREPEND \"#\")
  string(REPLACE \";\" \"\\n\" content \"\${content}\")
  string(REPLACE \"\${dummy}\" \";\" content \"\${content}\")
  file(WRITE
    \"${PROJECT_BINARY_DIR}/${CMAKE_INSTALL_SYSCONFDIR}/vast/vast.yaml\"
    \"\${content}\")")

add_custom_command(
  OUTPUT "${PROJECT_BINARY_DIR}/${CMAKE_INSTALL_SYSCONFDIR}/vast/vast.yaml"
  MAIN_DEPENDENCY "${PROJECT_SOURCE_DIR}/vast.yaml.example"
  COMMENT "Copying example configuration file"
  COMMAND ${CMAKE_COMMAND} -P
          "${CMAKE_CURRENT_BINARY_DIR}/example-config.cmake")

add_custom_target(
  copy-example-configuration-file
  DEPENDS "${PROJECT_BINARY_DIR}/${CMAKE_INSTALL_SYSCONFDIR}/vast/vast.yaml")

add_dependencies(vast copy-example-configuration-file)

install(FILES "${PROJECT_BINARY_DIR}/${CMAKE_INSTALL_SYSCONFDIR}/vast/vast.yaml"
        DESTINATION "${CMAKE_INSTALL_SYSCONFDIR}/vast/")

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
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/integration-$<CONFIG>.sh"
  CONTENT
    "#!/bin/sh
     if ! command -v jq >/dev/null 2>&1; then
       >&2 echo 'failed to find jq in $PATH'
       exit 1
     fi
     base_dir=\"${CMAKE_CURRENT_SOURCE_DIR}/integration\"
     env_dir=\"${CMAKE_CURRENT_BINARY_DIR}/integration_env\"
     app=\"$<IF:$<BOOL:${VAST_ENABLE_RELOCATABLE_INSTALLATIONS}>,$<TARGET_FILE:vast::vast>,${CMAKE_INSTALL_FULL_BINDIR}/$<TARGET_FILE_NAME:vast::vast>>\"
     set -e
     if [ ! -f \"$env_dir/bin/activate\" ]; then
       python3 -m venv \"$env_dir\"
     fi
     . \"$env_dir/bin/activate\"
     python -m pip install --upgrade pip
     python -m pip install -r \"$base_dir/requirements.txt\"
     python \"$base_dir/integration.py\" \
      --app \"$app\" \
      --directory vast-integration-test \
      --set \"$base_dir\"/vast_integration_suite.yaml \
      \"$@\"
     $<$<BOOL:${VAST_ENABLE_LSVAST}>:
     mkdir lsvast-integration-test
     $app -N -d lsvast-integration-test/vast.db import -r \"$base_dir\"/data/suricata/eve.json suricata
     python \"$base_dir/integration.py\" \
      --app \"$<TARGET_FILE:lsvast>\" \
      --directory lsvast-integration-test \
      --set \"$base_dir\"/lsvast_integration_suite.yaml \
      \"$@\">
    ")

add_custom_target(
  integration
  COMMAND /bin/sh "${CMAKE_CURRENT_BINARY_DIR}/integration-$<CONFIG>.sh" "-v"
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

# -- add plugins ---------------------------------------------------------------

set(VAST_PLUGINS
    ""
    CACHE STRING "Specify a list of plugins to build with VAST")
cmake_dependent_option(
  VAST_ENABLE_STATIC_PLUGINS "Force plugins to be linked statically" OFF
  "NOT VAST_ENABLE_STATIC_EXECUTABLE" ON)
add_feature_info("VAST_ENABLE_STATIC_PLUGINS" VAST_ENABLE_STATIC_PLUGINS
                 "Force plugins to be linked statically.")
if (VAST_PLUGINS)
  list(SORT VAST_PLUGINS)
  foreach (plugin_source_dir IN LISTS VAST_PLUGINS)
    if (NOT IS_ABSOLUTE "${plugin_source_dir}")
      string(PREPEND plugin_source_dir "${PROJECT_SOURCE_DIR}/")
    endif ()
    get_filename_component(plugin_binary_dir "${plugin_source_dir}" NAME)
    string(PREPEND plugin_binary_dir "${PROJECT_BINARY_DIR}/plugins/")
    add_subdirectory("${plugin_source_dir}" "${plugin_binary_dir}")
  endforeach ()
endif ()

get_property(VAST_BUNDLED_PLUGINS GLOBAL
             PROPERTY "VAST_BUNDLED_PLUGINS_PROPERTY")
list(TRANSFORM VAST_BUNDLED_PLUGINS PREPEND "\"")
list(TRANSFORM VAST_BUNDLED_PLUGINS APPEND "\"")
list(JOIN VAST_BUNDLED_PLUGINS "," joined_bundled_plugins)
target_compile_definitions(
  vast PRIVATE "VAST_BUNDLED_PLUGINS=${joined_bundled_plugins}")

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
      COMMAND ${CMAKE_BINARY_DIR}/bin/vast --bare-mode manual
      OUTPUT_FILE ${man_generated}.md)
    execute_process(
      COMMAND pandoc -s -f markdown -t man ${man_generated}.md
      OUTPUT_FILE ${man_generated})")
  add_custom_target(
    vast-man ALL
    DEPENDS vast
    BYPRODUCTS ${man_generated}
    COMMAND ${CMAKE_COMMAND} -P make_man_${man_page}.cmake
    COMMENT "Generating man page ${man_page}"
    VERBATIM)
  install(
    FILES "${man_generated}"
    DESTINATION "${CMAKE_INSTALL_MANDIR}/man1"
    OPTIONAL)
endif ()
