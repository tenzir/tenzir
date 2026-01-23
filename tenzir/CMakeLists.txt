add_executable(tenzir tenzir.cpp operator_new.cpp malloc.cpp)
# Static binaries are not fully static on macOS, they still need to link to the
# apple system libraries dynamically.
if (TENZIR_ENABLE_STATIC_EXECUTABLE AND NOT "${TENZIR_ALLOCATOR}" STREQUAL "none" AND NOT APPLE)
  target_link_options(
    tenzir
    BEFORE
    PRIVATE
    LINKER:--wrap=malloc
    LINKER:--wrap=calloc
    LINKER:--wrap=realloc
    LINKER:--wrap=reallocarray
    LINKER:--wrap=aligned_alloc
    LINKER:--wrap=malloc_usable_size
    LINKER:--wrap=memalign
    LINKER:--wrap=posix_memalign
    LINKER:--wrap=free)
endif ()

TenzirTargetEnableTooling(tenzir)
target_link_libraries(tenzir PRIVATE tenzir::internal tenzir::libtenzir)
TenzirTargetLinkWholeArchive(tenzir PRIVATE tenzir::libtenzir_builtins)
add_executable(tenzir::tenzir ALIAS tenzir)

# Install tenzir in PREFIX/lib and headers in PREFIX/include/tenzir.
install(
  TARGETS tenzir
  EXPORT TenzirTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Development
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT Runtime
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}" COMPONENT Runtime)

macro (create_tenzir_symlink name)
  # Implicitly create tenzir-ctl and tenzir-node symlinks when building tenzir.
  # That means that the links get re-created every time tenzir is built, but we
  # tolerate this overhead to avoid the inconvenience of missing links.
  add_custom_command(
    TARGET tenzir
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E create_symlink "./tenzir"
            "$<TARGET_FILE_DIR:tenzir>/${name}"
    COMMENT "Creating ${name} symlink"
    VERBATIM)
  # We want the clean target to remove the symlinks, but TARGET* generator
  # expressions aren't allowed in BYPRODUCTS. We can work around that with a
  # low-level mechanism.
  set_property(
    TARGET tenzir
    APPEND
    PROPERTY ADDITIONAL_CLEAN_FILES "$<TARGET_FILE_DIR:tenzir>/${name}")
  # The generated code in cmake_install.cmake for executable targets is
  # different depending on whether the CMAKE_INSTALL_BINDIR is set to an
  # absolute path or not. We emulate this behavior for the symlinks here so they
  # will always end up in the same location.
  if (NOT IS_ABSOLUTE "${CMAKE_INSTALL_BINDIR}")
    install(
      CODE "
      execute_process(COMMAND
        \"${CMAKE_COMMAND}\" -E create_symlink \"./tenzir\"
        \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/${name}\")"
      COMPONENT Runtime)
  else ()
    install(
      CODE "
      execute_process(COMMAND
        \"${CMAKE_COMMAND}\" -E create_symlink \"./tenzir\"
        \"\$ENV{DESTDIR}${CMAKE_INSTALL_FULL_BINDIR}/${name}\")"
      COMPONENT Runtime)
  endif ()
endmacro ()

create_tenzir_symlink(tenzir-node)
create_tenzir_symlink(tenzir-ctl)

# -- example configuration file -----------------------------------------------

TenzirInstallExampleConfiguration(
  tenzir "${PROJECT_SOURCE_DIR}/tenzir.yaml.example" "" "tenzir.yaml")

# -- init system integration ---------------------------------------------------

option(TENZIR_ENABLE_INIT_SYSTEM_INTEGRATION
       "Integrate with init systems when installing" ON)

if (TENZIR_ENABLE_INIT_SYSTEM_INTEGRATION)
  if (${CMAKE_SYSTEM_NAME} STREQUAL "FreeBSD")
    # Install rc.d script on FreeBSD into PREFIX/etc/rc.d.
    # TODO: Technically this can break for relocatable binaries when the install
    # prefix at build-time is different than the install prefix at install-time.
    # The macOS installation below handles this correctly, and the FreeBSD
    # installation should be adapted to work similarly.
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/services/rc.d/tenzir.in"
                   "${CMAKE_CURRENT_BINARY_DIR}/services/rc.d/tenzir" @ONLY)
    install(
      FILES "${CMAKE_CURRENT_BINARY_DIR}/services/rc.d/tenzir"
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
      set(TENZIR_BINARY \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/bin/$<TARGET_FILE_NAME:tenzir>\")
      set(TENZIR_WORKING_DIRECTORY \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/share/tenzir\")
      message(STATUS
        \"Installing: \$ENV{HOME}/Library/LaunchAgents/com.tenzir.tenzir.plist\")
      configure_file(
        \"${CMAKE_CURRENT_SOURCE_DIR}/services/launchd/com.tenzir.tenzir.plist.in\"
        \"\$ENV{HOME}/Library/LaunchAgents/com.tenzir.tenzir.plist\" @ONLY)"
      COMPONENT Runtime)
  elseif (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    # Not all Linux distros use systemd, but those we care about at the moment do.
    # TODO: We don't consider CPACK_PACKAGE_INSTALL_PREFIX here, this could
    # potentially be solved with a more involved CPack setup. See also the
    # comment at the beginning of cmake/Package.cmake.
    configure_file(
      "${CMAKE_CURRENT_SOURCE_DIR}/services/systemd/tenzir-node.service.in"
      "${CMAKE_CURRENT_BINARY_DIR}/tenzir-node.service" @ONLY)
    install(
      FILES "${CMAKE_CURRENT_BINARY_DIR}/tenzir-node.service"
      DESTINATION "${CMAKE_INSTALL_LIBDIR}/systemd/system/"
      COMPONENT Runtime)
  endif ()
endif ()

# Note: We use CTest's built-in mechanisms instead of recursive directory traversal
# to avoid complexity and potential circular reference issues

set(plugins_default "plugins/*")
if (EXISTS ${PROJECT_SOURCE_DIR}/contrib/tenzir-plugins/.git)
  list(APPEND plugins_default "contrib/tenzir-plugins/*")
endif ()

set(TENZIR_PLUGINS
    ${plugins_default}
    CACHE STRING "Specify a list of plugin paths to build with Tenzir (supports
    globbing)")
set(TENZIR_PLUGINS_BLACKLIST
    ""
    CACHE
      STRING
      "Specify a list of plugin names to remove after the globs in TENZIR_PLUGINS
      are expanded. (does NOT support globbing)")
cmake_dependent_option(
  TENZIR_ENABLE_STATIC_PLUGINS "Force plugins to be linked statically" OFF
  "NOT TENZIR_ENABLE_STATIC_EXECUTABLE" ON)
if (TENZIR_PLUGINS)
  foreach (plugin_source_glob IN LISTS TENZIR_PLUGINS)
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
    if (NOT IS_DIRECTORY "${plugin_source_dir}"
        OR NOT EXISTS "${plugin_source_dir}/CMakeLists.txt")
      continue()
    endif ()
    get_filename_component(plugin_source_name "${plugin_source_dir}" NAME)
    if (plugin_source_name IN_LIST TENZIR_PLUGINS_BLACKLIST)
      continue()
    endif ()
    get_filename_component(plugin_binary_dir "${plugin_source_dir}" NAME)
    string(PREPEND plugin_binary_dir "${PROJECT_BINARY_DIR}/plugins/")
    add_subdirectory("${plugin_source_dir}" "${plugin_binary_dir}")
  endforeach ()
endif ()

# -- ca-certificates -----------------------------------------------------------

# On macOS there is no system install ca-certificate bundle by default, so we
# make it possbile to bundle one (usually curl's).
if (TENZIR_CACERT)
  install(
    FILES "${TENZIR_CACERT}"
    DESTINATION "${CMAKE_INSTALL_DATADIR}/tenzir/ca-bundle.crt"
    COMPONENT Runtime)
endif ()

# -- integration tests ---------------------------------------------------------

# Helper function to build tenzir-test command invocations
function (tenzir_build_test_command out_var)
  cmake_parse_arguments(ARG "" "TEST_ROOT" "EXTRA_ENV;EXTRA_ARGS;SELECTION"
                        ${ARGN})

  set(cmd
      "${CMAKE_COMMAND}"
      "-E"
      "chdir"
      "${CMAKE_SOURCE_DIR}/test"
      "${CMAKE_COMMAND}"
      "-E"
      "env"
      "--modify"
      "PATH=path_list_prepend:$<TARGET_FILE_DIR:tenzir::tenzir>")

  # Add extra environment variables if provided
  foreach (env_var IN LISTS ARG_EXTRA_ENV)
    list(APPEND cmd "${env_var}")
  endforeach ()

  list(
    APPEND
    cmd
    "TENZIR_BINARY=$<TARGET_FILE:tenzir::tenzir>"
    "TENZIR_NODE_BINARY=$<TARGET_FILE_DIR:tenzir::tenzir>/tenzir-node"
    "--"
    "${TENZIR_UV_PATH}"
    "tool"
    "run"
    "--python"
    ">=3.12"
    "tenzir-test")

  # Add extra arguments if provided
  if (ARG_EXTRA_ARGS)
    list(APPEND cmd ${ARG_EXTRA_ARGS})
  endif ()

  # Build test selection (positional arguments to tenzir-test)
  set(selection)

  if (ARG_TEST_ROOT)
    list(APPEND selection "${ARG_TEST_ROOT}")
  elseif (ARG_SELECTION)
    list(APPEND selection ${ARG_SELECTION})
  else ()
    # Default: all projects (test root + contrib plugins)
    list(APPEND cmd "--all-projects")
    list(APPEND selection "--root" "${CMAKE_SOURCE_DIR}/test")

    # Add contrib/tenzir-plugins/*/test directories if they exist
    if (EXISTS "${CMAKE_SOURCE_DIR}/contrib/tenzir-plugins")
      file(GLOB plugin_test_dirs
           "${CMAKE_SOURCE_DIR}/contrib/tenzir-plugins/*/test")
      if (plugin_test_dirs)
        list(APPEND selection ${plugin_test_dirs})
      endif ()
    endif ()
  endif ()

  list(APPEND cmd ${selection})
  set(${out_var}
      "${cmd}"
      PARENT_SCOPE)
endfunction ()

add_test(NAME build-tenzir
         COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --config
                 "$<CONFIG>" --target tenzir)
set_tests_properties(
  build-tenzir PROPERTIES FIXTURES_SETUP "tenzir_integration_test_fixture"
                          LABELS integration)

set(_integration_tests_root "${CMAKE_SOURCE_DIR}/test/tests")
unset(_integration_test_dirs)
if (EXISTS "${_integration_tests_root}")
  file(
    GLOB _integration_test_dirs
    RELATIVE "${_integration_tests_root}"
    "${_integration_tests_root}/[^_]*")
endif ()
foreach (dir IN LISTS _integration_test_dirs)
  if (NOT IS_DIRECTORY "${_integration_tests_root}/${dir}")
    continue()
  endif ()
  if ("${dir}" MATCHES "^inputs/?$")
    continue()
  endif ()
  tenzir_build_test_command(_test_cmd TEST_ROOT
                            "${_integration_tests_root}/${dir}/")
  add_test(NAME "tenzir/${dir}" COMMAND ${_test_cmd})
  set_tests_properties(
    "tenzir/${dir}"
    PROPERTIES WORKING_DIRECTORY
               "${CMAKE_SOURCE_DIR}/test"
               FIXTURES_REQUIRED
               "tenzir_integration_test_fixture"
               LABELS
               integration
               RUN_SERIAL
               TRUE)
endforeach ()

add_custom_target(integration-tests DEPENDS tenzir)
tenzir_build_test_command(_integration_tests_cmd EXTRA_ARGS "--jobs" "4"
                          "--summary")
add_custom_command(
  TARGET integration-tests
  POST_BUILD
  COMMAND ${_integration_tests_cmd}
  USES_TERMINAL VERBATIM)

add_custom_target(integration)
add_dependencies(integration integration-tests)

add_custom_target(update-integration DEPENDS tenzir)
tenzir_build_test_command(_update_integration_cmd EXTRA_ARGS "--update")
add_custom_command(
  TARGET update-integration
  POST_BUILD
  COMMAND ${_update_integration_cmd}
  USES_TERMINAL VERBATIM)

if (TENZIR_ENABLE_CODE_COVERAGE)
  if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(FATAL_ERROR "Code coverage is only supported in Debug builds")
  endif ()

  add_custom_target(integration-coverage DEPENDS tenzir)
  tenzir_build_test_command(
    _integration_coverage_cmd
    EXTRA_ENV
    "CMAKE_COVERAGE_OUTPUT_DIRECTORY=${CMAKE_COVERAGE_OUTPUT_DIRECTORY}"
    "COVERAGE_SOURCE_DIR=${CMAKE_SOURCE_DIR}"
    EXTRA_ARGS
    "--coverage"
    "--coverage-source-dir=${CMAKE_SOURCE_DIR}")
  add_custom_command(
    TARGET integration-coverage
    POST_BUILD
    COMMAND ${_integration_coverage_cmd}
    COMMAND
      "${CMAKE_COMMAND}" "-E" "env"
      "VERBOSE=$<IF:$<BOOL:${CMAKE_VERBOSE_MAKEFILE}>,1,0>" "--"
      "${CMAKE_SOURCE_DIR}/scripts/process_coverage.sh"
      "--coverage-dir=${CMAKE_COVERAGE_OUTPUT_DIRECTORY}"
      "--source-dir=${CMAKE_SOURCE_DIR}" "--binary-dir=${CMAKE_BINARY_DIR}"
      "--tenzir-binary=$<TARGET_FILE:tenzir>"
      "--llvm-profdata=${LLVM_PROFDATA_PATH}" "--llvm-cov=${LLVM_COV_PATH}"
    COMMENT "Running integration tests with code coverage enabled"
    USES_TERMINAL VERBATIM)
endif ()

if (BUILD_TESTING)
  # Get all tests using CTest's own test enumeration
  # This approach is simpler and avoids directory traversal issues
  get_property(
    _all_tests_prop
    DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    PROPERTY TESTS)
  foreach (_test IN LISTS _all_tests_prop)
    get_property(
      _labels
      TEST "${_test}"
      PROPERTY LABELS)
    if (_labels AND "integration" IN_LIST _labels)
      list(APPEND _integration_tests "${_test}")
    else ()
      list(APPEND _non_integration_tests "${_test}")
    endif ()
  endforeach ()
  if (_integration_tests AND _non_integration_tests)
    list(REMOVE_DUPLICATES _non_integration_tests)
    foreach (_integration_test IN LISTS _integration_tests)
      set_property(TEST "${_integration_test}"
                   PROPERTY DEPENDS "${_non_integration_tests}")
    endforeach ()
  endif ()
  unset(_integration_tests)
  unset(_non_integration_tests)
  unset(_all_tests_prop)
endif ()

get_property(TENZIR_BUNDLED_PLUGINS GLOBAL
             PROPERTY "TENZIR_BUNDLED_PLUGINS_PROPERTY")
list(TRANSFORM TENZIR_BUNDLED_PLUGINS PREPEND "\"")
list(TRANSFORM TENZIR_BUNDLED_PLUGINS APPEND "\"")
list(JOIN TENZIR_BUNDLED_PLUGINS "," joined_bundled_plugins)
target_compile_definitions(
  tenzir PRIVATE "TENZIR_BUNDLED_PLUGINS=${joined_bundled_plugins}")

# -- man page ------------------------------------------------------------------

# TODO: Feed the last commit date in. Using the current date breaks
# deterministic builds.
string(TIMESTAMP TENZIR_LAST_COMMIT_DATE)
if (TENZIR_ENABLE_MANPAGES)
  add_custom_target(
    tenzir-manpage
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/tenzir.1.md"
    COMMAND
      "${PANDOC}" -s -f gfm -t man -V date:${TENZIR_LAST_COMMIT_DATE}
      "${CMAKE_CURRENT_SOURCE_DIR}/tenzir.1.md" -o
      "${CMAKE_CURRENT_BINARY_DIR}/tenzir.1"
    BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/tenzir.1"
    COMMENT "Generating tenzir.1"
    VERBATIM)
  add_dependencies(tenzir tenzir-manpage)
  install(
    FILES "${CMAKE_CURRENT_BINARY_DIR}/tenzir.1"
    DESTINATION "${CMAKE_INSTALL_MANDIR}/man1"
    COMPONENT Runtime)
endif ()
