include_guard(GLOBAL)

# Support tools like clang-tidy by creating a compilation database and copying
# it to the project root.
function (VASTExportCompileCommands)
  if (TARGET compilation-database)
    message(FATAL_ERROR "VASTExportCompileCommands must be called only once")
  endif ()

  if (NOT ${ARGC} EQUAL 1)
    message(FATAL_ERROR "VASTExportCompileCommands takes exactly one argument")
  endif ()

  if (NOT TARGET ${ARGV0})
    message(FATAL_ERROR "VASTExportCompileCommands provided invalid target")
  endif ()

  # Globally enable compilation databases.
  # TODO: Use the EXPORT_COMPILE_COMMANDS property when using CMake >= 3.20.
  set(CMAKE_EXPORT_COMPILE_COMMANDS
      ON
      PARENT_SCOPE)

  # Link once when configuring the build to make the compilation database
  # immediately available.
  execute_process(
    COMMAND
      ${CMAKE_COMMAND} -E create_symlink
      "${CMAKE_BINARY_DIR}/compile_commands.json"
      "${CMAKE_SOURCE_DIR}/compile_commands.json")

  # Link again when building the specified target. This ensures the file is
  # available even after the user ran `git-clean` or similar and triggered
  # another build.
  add_custom_target(
    compilation-database
    BYPRODUCTS "${CMAKE_SOURCE_DIR}/compile_commands.json"
    COMMAND
      ${CMAKE_COMMAND} -E create_symlink
      "${CMAKE_BINARY_DIR}/compile_commands.json"
      "${CMAKE_SOURCE_DIR}/compile_commands.json"
    COMMENT "Linking compilation database for ${ARGV0} to ${CMAKE_SOURCE_DIR}")
  add_dependencies(${ARGV0} compilation-database)
endfunction ()

function (VASTRegisterPlugin)
  cmake_parse_arguments(
    # <prefix>
    PLUGIN
    # <options>
    ""
    # <one_value_keywords>
    "TARGET;ENTRYPOINT"
    # <multi_value_keywords>
    "SOURCES;TEST_SOURCES;INCLUDE_DIRECTORIES"
    # <args>...
    ${ARGN})

  # Safeguard against repeated VASTRegisterPlugin calls from the same project.
  # While technically possible for external pluigins, doing so makes it
  # impossible to build the plugin alongside VAST.
  get_property(VAST_PLUGIN_PROJECT_SOURCE_DIRS GLOBAL
               PROPERTY "VAST_PLUGIN_PROJECT_SOURCE_DIRS_PROPERTY")
  if ("${PROJECT_SOURCE_DIR}" IN_LIST VAST_PLUGIN_PROJECT_SOURCE_DIRS)
    message(
      FATAL_ERROR
        "VASTRegisterPlugin called twice in CMake project ${PROJECT_NAME}")
  endif ()
  set_property(GLOBAL APPEND PROPERTY "VAST_PLUGIN_PROJECT_SOURCE_DIRS_PROPERTY"
                                      "${PROJECT_SOURCE_DIR}")

  # Set a default build type if none was specified
  if (NOT "${CMAKE_PROJECT_NAME}" STREQUAL "VAST")
    set(default_build_type "Release")
    if (EXISTS "${CMAKE_SOURCE_DIR}/.git")
      set(default_build_type "Debug")
    endif ()

    if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
      message(
        STATUS
          "Setting build type to '${default_build_type}' as none was specified."
      )
      set(CMAKE_BUILD_TYPE
          "${default_build_type}"
          CACHE STRING "Choose the type of build." FORCE)
      # Set the possible values of build type for cmake-gui
      set_property(
        CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel"
                                        "RelWithDebInfo")
    endif ()
  endif ()

  # Provides install directory variables as defined for GNU software:
  # http://www.gnu.org/prep/standards/html_node/Directory-Variables.html
  include(GNUInstallDirs)

  # A replacement for target_link_libraries that links static libraries using
  # the platform-specific whole-archive options. Please test any changes to this
  # macro on all supported platforms and compilers.
  macro (target_link_whole_archive target visibility library)
    get_target_property(target_type ${library} TYPE)
    if (target_type STREQUAL "STATIC_LIBRARY")
      # Prevent elision of self-registration code in statically linked libraries,
      # c.f., https://www.bfilipek.com/2018/02/static-vars-static-lib.html
      # Possible PLATFORM_ID values:
      # - Windows: Windows (Visual Studio, MinGW GCC)
      # - Darwin: macOS/OS X (Clang, GCC)
      # - Linux: Linux (GCC, Intel, PGI)
      # - Android: Android NDK (GCC, Clang)
      # - FreeBSD: FreeBSD
      # - CrayLinuxEnvironment: Cray supercomputers (Cray compiler)
      # - MSYS: Windows (MSYS2 shell native GCC)#
      target_link_options(
        ${target}
        ${visibility}
        $<$<PLATFORM_ID:Darwin>:LINKER:-force_load,$<TARGET_FILE:${library}>>
        $<$<OR:$<PLATFORM_ID:Linux>,$<PLATFORM_ID:FreeBSD>>:LINKER:--whole-archive,$<TARGET_FILE:${library}>,--no-whole-archive>
        $<$<PLATFORM_ID:Windows>:LINKER:/WHOLEARCHIVE,$<TARGET_FILE:${library}>>
      )
    endif ()
    target_link_libraries(${target} ${visibility} ${library})
  endmacro ()

  macro (make_absolute vars)
    foreach (var IN LISTS "${vars}")
      get_filename_component(var_abs "${var}" ABSOLUTE)
      list(APPEND vars_abs "${var_abs}")
    endforeach ()
    set("${vars}" "${vars_abs}")
    unset(vars)
    unset(vars_abs)
  endmacro ()

  if (NOT PLUGIN_TARGET)
    message(
      FATAL_ERROR "TARGET must be specified in call to VASTRegisterPlugin")
  endif ()

  if (NOT PLUGIN_ENTRYPOINT)
    list(LENGTH PLUGIN_SOURCES num_sources)
    if (num_sources EQUAL 1)
      list(GET PLUGIN_SOURCES 0 PLUGIN_ENTRYPOINT)
      set(PLUGIN_SOURCEs "")
    else ()
      message(
        FATAL_ERROR "ENTRYPOINT must be specified in call to VASTRegisterPlugin"
      )
    endif ()
  endif ()

  # Make all given paths absolute.
  make_absolute(PLUGIN_ENTRYPOINT)
  make_absolute(PLUGIN_SOURCES)
  make_absolute(PLUGIN_INCLUDE_DIRECTORIES)

  # Deduplicate the entrypoint so plugin authors can grep for sources while
  # still specifying the entrypoint manually.
  if ("${PLUGIN_ENTRYPOINT}" IN_LIST PLUGIN_SOURCES)
    list(REMOVE_ITEM PLUGIN_SOURCES "${PLUGIN_ENTRYPOINT}")
  endif ()

  # Create an object library target for our plugin _without_ the entrypoint.
  file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/stub.h" "")
  list(APPEND PLUGIN_SOURCES "${CMAKE_CURRENT_BINARY_DIR}/stub.h")
  add_library(${PLUGIN_TARGET} OBJECT ${PLUGIN_SOURCES})
  target_link_libraries(
    ${PLUGIN_TARGET}
    PUBLIC vast::libvast
    PRIVATE vast::internal)

  # Set up the target's include directories.
  if (PLUGIN_INCLUDE_DIRECTORIES)
    # We intentionally omit the install interface include directories and do not
    # install the given include directories, as installed plugin libraries are
    # not meant to be developed against.
    list(JOIN PLUGIN_INCLUDE_DIRECTORIES " " include_directories)
    target_include_directories(${PLUGIN_TARGET}
                               PUBLIC $<BUILD_INTERFACE:${include_directories}>)
  endif ()

  # Create a static library target for our plugin with the entrypoint, and use
  # static versions of VAST_REGISTER_PLUGIN family of macros.
  add_library(${PLUGIN_TARGET}-static STATIC ${PLUGIN_ENTRYPOINT})
  target_link_whole_archive(${PLUGIN_TARGET}-static PRIVATE ${PLUGIN_TARGET})
  target_link_libraries(${PLUGIN_TARGET}-static PRIVATE vast::internal)
  target_compile_definitions(${PLUGIN_TARGET}-static
                             PRIVATE VAST_ENABLE_STATIC_PLUGINS)

  if (VAST_ENABLE_STATIC_PLUGINS)
    # Link our static library against the vast binary directly.
    target_link_whole_archive(vast PRIVATE ${PLUGIN_TARGET}-static)
  else ()
    # Enable position-independent code for the static library if we're linking
    # it into shared one.
    set_property(TARGET ${PLUGIN_TARGET} PROPERTY POSITION_INDEPENDENT_CODE ON)

    # Create a shared library target for our plugin.
    add_library(${PLUGIN_TARGET}-shared SHARED ${PLUGIN_ENTRYPOINT})
    target_link_whole_archive(${PLUGIN_TARGET}-shared PRIVATE ${PLUGIN_TARGET})
    target_link_libraries(${PLUGIN_TARGET}-shared PRIVATE vast::internal)

    # Install the plugin library to <libdir>/vast/plugins, and also configure
    # the library output directory accordingly.
    if (IS_ABSOLUTE "${CMAKE_INSTALL_LIBDIR}")
      message(FATAL_ERROR "CMAKE_INSTALL_LIBDIR must be a relative path")
    endif ()
    set_target_properties(
      ${PLUGIN_TARGET}-shared
      PROPERTIES LIBRARY_OUTPUT_DIRECTORY
                 "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR}/vast/plugins"
                 OUTPUT_NAME "vast-plugin-${PLUGIN_TARGET}")
    install(TARGETS ${PLUGIN_TARGET}-shared
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/vast/plugins")

    # Ensure that VAST only runs after all dynamic plugin libraries are built.
    if (TARGET vast)
      add_dependencies(vast ${PLUGIN_TARGET}-shared)
    endif ()
  endif ()

  if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/schema")
    # Install the bundled schema files to <datadir>/vast.
    install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/schema"
            DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/plugin/${PLUGIN_TARGET}")
    # Copy schemas from bundled plugins to the build directory so they can be
    # used from a VAST in a build directory (instead if just an installed VAST).
    file(
      GLOB_RECURSE
      plugin_schema_files
      CONFIGURE_DEPENDS
      "${CMAKE_CURRENT_SOURCE_DIR}/schema/*.schema"
      "${CMAKE_CURRENT_SOURCE_DIR}/schema/*.yml"
      "${CMAKE_CURRENT_SOURCE_DIR}/schema/*.yaml")
    list(SORT plugin_schema_files)
    foreach (plugin_schema_file IN LISTS plugin_schema_files)
      string(REGEX REPLACE "^${CMAKE_CURRENT_SOURCE_DIR}/schema/" ""
                           relative_plugin_schema_file ${plugin_schema_file})
      string(MD5 plugin_schema_file_hash "${plugin_schema_file}")
      add_custom_target(
        vast-schema-${plugin_schema_file_hash} ALL
        BYPRODUCTS
          "${CMAKE_BINARY_DIR}/share/vast/plugin/${PLUGIN_TARGET}/schema/${relative_plugin_schema_file}"
        COMMAND
          "${CMAKE_COMMAND}" -E copy "${plugin_schema_file}"
          "${CMAKE_BINARY_DIR}/share/vast/plugin/${PLUGIN_TARGET}/schema/${relative_plugin_schema_file}"
        COMMENT
          "Copying schema file ${relative_plugin_schema_file} for plugin ${PLUGIN_TARGET}"
      )
      if (TARGET vast-schema)
        add_dependencies(vast-schema vast-schema-${plugin_schema_file_hash})
      endif ()
    endforeach ()
  endif ()

  # Setup unit tests.
  if (VAST_ENABLE_UNIT_TESTS AND PLUGIN_TEST_SOURCES)
    add_executable(${PLUGIN_TARGET}-test ${PLUGIN_TEST_SOURCES})
    target_link_libraries(${PLUGIN_TARGET}-test PRIVATE vast::test
                                                        vast::internal)
    target_link_whole_archive(${PLUGIN_TARGET}-test PRIVATE
                              ${PLUGIN_TARGET}-static)
    add_test(NAME build-${PLUGIN_TARGET}-test
             COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --config
                     "$<CONFIG>" --target ${PLUGIN_TARGET}-test)
    set_tests_properties(build-${PLUGIN_TARGET}-test
                         PROPERTIES FIXTURES_SETUP vast_unit_test_fixture)
    add_test(NAME ${PLUGIN_TARGET} COMMAND ${PLUGIN_TARGET}-test)
    set_tests_properties(${PLUGIN_TARGET} PROPERTIES FIXTURES_REQUIRED
                                                     vast_unit_test_fixture)
  endif ()

  # Setup integration tests.
  if (TARGET vast::vast
      AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/integration/tests.yaml")
    if ("${CMAKE_PROJECT_NAME}" STREQUAL "VAST")
      set(integration_test_path "${CMAKE_SOURCE_DIR}/vast/integration")
    else ()
      if (IS_ABSOLUTE "${CMAKE_INSTALL_DATADIR}")
        set(integration_test_path "${CMAKE_INSTALL_DATADIR}/vast/integration")
      else ()
        get_target_property(integration_test_path vast::vast LOCATION)
        get_filename_component(integration_test_path "${integration_test_path}"
                               DIRECTORY)
        get_filename_component(integration_test_path "${integration_test_path}"
                               DIRECTORY)
        set(integration_test_path
            "${integration_test_path}/${CMAKE_INSTALL_DATADIR}/vast/integration"
        )
      endif ()
    endif ()
    file(
      GENERATE
      OUTPUT
        "${CMAKE_CURRENT_BINARY_DIR}/${PLUGIN_TARGET}-integration-$<CONFIG>.sh"
      CONTENT
        "#!/bin/sh
        if ! command -v jq >/dev/null 2>&1; then
          >&2 echo 'failed to find jq in $PATH'
          exit 1
        fi
        base_dir=\"${integration_test_path}\"
        env_dir=\"${CMAKE_CURRENT_BINARY_DIR}/integration_env\"
        app=\"$<TARGET_FILE:vast::vast>\"
        set -e
        if [ ! -f \"$env_dir/bin/activate\" ]; then
          python3 -m venv \"$env_dir\"
        fi
        . \"$env_dir/bin/activate\"
        python -m pip install --upgrade pip
        python -m pip install -r \"$base_dir/requirements.txt\"
        $<$<BOOL:${VAST_ENABLE_ARROW}>:python -m pip install pyarrow>
        $<$<TARGET_EXISTS:${PLUGIN_TARGET}-shared>:export VAST_PLUGIN_DIRS=\"$<TARGET_FILE_DIR:${PLUGIN_TARGET}-shared>\">
        export VAST_SCHEMA_DIRS=\"${CMAKE_CURRENT_SOURCE_DIR}/schema\"
        python \"$base_dir/integration.py\" \
          --app \"$app\" \
          --set \"${CMAKE_CURRENT_SOURCE_DIR}/integration/tests.yaml\" \
          --directory vast-${PLUGIN_TARGET}-integration-test \
          \"$@\"")
    add_custom_target(
      ${PLUGIN_TARGET}-integration
      COMMAND
        /bin/sh
        "${CMAKE_CURRENT_BINARY_DIR}/${PLUGIN_TARGET}-integration-$<CONFIG>.sh"
        -v DEBUG
      USES_TERMINAL)
    if (NOT TARGET integration)
      add_custom_target(integration)
    endif ()
    add_dependencies(integration ${PLUGIN_TARGET}-integration)
  endif ()

  if ("${CMAKE_PROJECT_NAME}" STREQUAL "VAST")
    # Provide niceties for building alongside VAST.
    dependency_summary("${PLUGIN_TARGET}" "${CMAKE_CURRENT_LIST_DIR}" "Plugins")
    set_property(GLOBAL APPEND PROPERTY "VAST_BUNDLED_PLUGINS_PROPERTY"
                                        "${PLUGIN_TARGET}")
  else ()
    # Provide niceties for external plugins that are usually part of VAST.
    VASTExportCompileCommands(${PLUGIN_TARGET})
  endif ()
endfunction ()
