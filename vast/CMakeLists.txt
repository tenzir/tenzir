add_executable(tenzir tenzir.cpp)
VASTTargetEnableTooling(tenzir)
target_link_libraries(tenzir PRIVATE vast::internal vast::libvast)
VASTTargetLinkWholeArchive(tenzir PRIVATE vast::libvast_builtins)
install(
  TARGETS tenzir
  DESTINATION "${CMAKE_INSTALL_BINDIR}"
  COMPONENT Runtime)
add_executable(vast::tenzir ALIAS tenzir)

# Install vast in PREFIX/lib and headers in PREFIX/include/vast.
install(
  TARGETS tenzir
  EXPORT VASTTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Runtime
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT Runtime)

macro (create_tenzir_symlink name)
  add_custom_command(
    OUTPUT "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_BINDIR}/${name}"
    COMMAND ${CMAKE_COMMAND} -E create_symlink "./tenzir"
            "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_BINDIR}/${name}"
    DEPENDS tenzir
    COMMENT "Creating ${name} symlink")
  add_custom_target(
    "${name}" ALL DEPENDS "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_BINDIR}/${name}")
  install(
    CODE "
    execute_process(COMMAND
      \"${CMAKE_COMMAND}\" -E create_symlink \"./tenzir\"
      \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/${name}\")"
    COMPONENT Runtime)
endmacro ()

create_tenzir_symlink(tenzir-node)
create_tenzir_symlink(tenzir-ctl)
create_tenzir_symlink(vast)

# -- example configuration file -----------------------------------------------

VASTInstallExampleConfiguration(vast "${PROJECT_SOURCE_DIR}/tenzir.yaml.example"
                                "" "vast.yaml")

# -- init system integration ---------------------------------------------------

# TODO: This should be behind an option, and work for Linux as well.

if (${CMAKE_SYSTEM_NAME} STREQUAL "FreeBSD")
  # Install rc.d script on FreeBSD into PREFIX/etc/rc.d.
  # TODO: Technically this can break for relocatable binaries when the install
  # prefix at build-time is different than the install prefix at install-time.
  # The macOS installation below handles this correctly, and the FreeBSD
  # installation should be adapted to work similarly.
  configure_file("${CMAKE_CURRENT_SOURCE_DIR}/services/rc.d/vast.in"
                 "${CMAKE_CURRENT_BINARY_DIR}/services/rc.d/vast" @ONLY)
  install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/services/rc.d/vast"
    DESTINATION "${CMAKE_INSTALL_SYSCONFDIR}/rc.d"
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_EXECUTE
    COMPONENT Runtime)
elseif (${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  # Install launchd script on macOS into ~/Library/LaunchAgents
  install(
    CODE "\
    if (NOT IS_ABSOLUTE \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}\")
      string(PREPEND CMAKE_INSTALL_PREFIX \"${CMAKE_SOURCE_DIR}/\")
    endif ()
    set(VAST_BINARY \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/bin/$<TARGET_FILE_NAME:tenzir>\")
    set(VAST_WORKING_DIRECTORY \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/share/vast\")
    message(STATUS
      \"Installing: \$ENV{HOME}/Library/LaunchAgents/com.tenzir.vast.plist\")
    configure_file(
      \"${CMAKE_CURRENT_SOURCE_DIR}/services/launchd/com.tenzir.vast.plist.in\"
      \"\$ENV{HOME}/Library/LaunchAgents/com.tenzir.vast.plist\" @ONLY)"
    COMPONENT Runtime)
elseif (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  # Not all Linux distros use systemd, but those we care about at the moment do.
  # TODO: We don't consider CPACK_PACKAGE_INSTALL_PREFIX here, this could
  # potentially be solved with a more involved CPack setup. See also the
  # comment at the beginning of cmake/Package.cmake.
  configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/services/systemd/tenzir.service.in"
    "${CMAKE_CURRENT_BINARY_DIR}/tenzir.service" @ONLY)
  install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/tenzir.service"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/systemd/system/"
    COMPONENT Runtime)
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
     export app=\"$<IF:$<BOOL:${VAST_ENABLE_RELOCATABLE_INSTALLATIONS}>,$<TARGET_FILE:vast::tenzir>,${CMAKE_INSTALL_FULL_BINDIR}/$<TARGET_FILE_NAME:vast::tenzir>>-ctl\"
     update=\"$<IF:$<BOOL:${VAST_ENABLE_UPDATE_INTEGRATION_REFERENCES}>,-u,>\"
     set -e
     if [ ! -f \"$env_dir/bin/activate\" ]; then
       python3 -m venv \"$env_dir\"
     fi
     . \"$env_dir/bin/activate\"
     python3 -m pip install --upgrade pip
     python3 -m pip install -r \"$base_dir/requirements.txt\"
     python3 \"$base_dir/integration.py\" \
      --app \"$app\" \
      --directory vast-integration-test \
      --set \"$base_dir\"/vast_integration_suite.yaml \
      \${update} \
      \"$@\"
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
  COMPONENT Development
  DESTINATION "${CMAKE_INSTALL_DATADIR}/vast"
  PATTERN "integration/tenzir.yaml.example" EXCLUDE)

install(
  FILES "${PROJECT_SOURCE_DIR}/tenzir.yaml.example"
  COMPONENT Development
  DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/integration")

# -- add plugins ---------------------------------------------------------------

set(plugins_default "plugins/*")
if (EXISTS contrib/vast-plugins/.git)
  list(APPEND plugins_default "contrib/vast-plugins/*")
endif ()

set(VAST_PLUGINS
    ${plugins_default}
    CACHE STRING "Specify a list of plugins to build with VAST (supports
    globbing)")
cmake_dependent_option(
  VAST_ENABLE_STATIC_PLUGINS "Force plugins to be linked statically" OFF
  "NOT VAST_ENABLE_STATIC_EXECUTABLE" ON)
add_feature_info("VAST_ENABLE_STATIC_PLUGINS" VAST_ENABLE_STATIC_PLUGINS
                 "force plugins to be linked statically.")
if (VAST_PLUGINS)
  foreach (plugin_source_glob IN LISTS VAST_PLUGINS)
    if (IS_ABSOLUTE "${plugin_source_glob}")
      list(APPEND plugin_source_globs "${plugin_source_glob}")
    else ()
      list(APPEND plugin_source_globs
           "${PROJECT_SOURCE_DIR}/${plugin_source_glob}")
    endif ()
  endforeach ()
  file(
    GLOB plugin_source_dirs
    LIST_DIRECTORIES ON
    CONFIGURE_DEPENDS ${plugin_source_globs})
  list(SORT plugin_source_dirs)
  foreach (plugin_source_dir IN LISTS plugin_source_dirs)
    if (NOT IS_DIRECTORY "${plugin_source_dir}")
      continue()
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
  tenzir PRIVATE "VAST_BUNDLED_PLUGINS=${joined_bundled_plugins}")

# -- man page ------------------------------------------------------------------

# TODO: Feed the last commit date in. Using the current date breaks
# deterministic builds.
string(TIMESTAMP VAST_LAST_COMMIT_DATE)
if (VAST_ENABLE_MANPAGES)
  add_custom_target(
    vast-manpage
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/vast.1.md"
    COMMAND
      "${PANDOC}" -s -f gfm -t man -V date:${VAST_LAST_COMMIT_DATE}
      "${CMAKE_CURRENT_SOURCE_DIR}/vast.1.md" -o
      "${CMAKE_CURRENT_BINARY_DIR}/vast.1"
    BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/vast.1"
    COMMENT "Generating vast.1"
    VERBATIM)
  add_dependencies(vast vast-manpage)
  install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/vast.1"
    DESTINATION "${CMAKE_INSTALL_MANDIR}/man1"
    COMPONENT Runtime)
endif ()
