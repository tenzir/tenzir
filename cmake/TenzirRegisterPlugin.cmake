include_guard(GLOBAL)

find_package(Git)

# Define a target for updating integration test references.
macro (TenzirDefineUpdateIntegrationTarget _target _references_dir)
  add_custom_target(
    update-${_target}
    COMMAND "${CMAKE_COMMAND}" -E env UPDATE=1 "${CMAKE_COMMAND}" --build
            "${CMAKE_BINARY_DIR}" --target ${_target}
    COMMENT "Updating ${_target} test references..."
    USES_TERMINAL)
  if (NOT EXISTS "${_references_dir}")
    message(STATUS "disabling diff-${_target}")
  elseif (Git_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
    add_custom_target(
      diff-${_target}
      COMMAND "${GIT_EXECUTABLE}" "-C" "${_references_dir}" "diff" "--exit-code"
              "--" "${_references_dir}/**/*.ref"
      DEPENDS update-${_target}
      COMMENT "Diffing integration test results for ${_target}"
      USES_TERMINAL)
  endif ()
endmacro ()

# Normalize the GNUInstallDirs to be relative paths, if possible.
macro (TenzirNormalizeInstallDirs)
  foreach (
    _install IN
    ITEMS "BIN"
          "SBIN"
          "LIBEXEC"
          "SYSCONF"
          "SHAREDSTATE"
          "LOCALSTATE"
          "RUNSTATE"
          "LIB"
          "INCLUDE"
          # "OLDINCLUDE" <- deliberately omitted
          "DATAROOT"
          "DATA"
          "INFO"
          "LOCALE"
          "MAN"
          "DOC")
    # Try removing CMAKE_INSTALL_PREFIX with a trailing slash from the full
    # path to get the correct relative path because some package managers always
    # invoke CMake with absolute install paths even when they all share a common
    # prefix.
    if (IS_ABSOLUTE "${CMAKE_INSTALL_${_install}DIR}")
      string(
        REGEX
        REPLACE "^${CMAKE_INSTALL_PREFIX}/" "" "CMAKE_INSTALL_${_install}DIR"
                "${CMAKE_INSTALL_FULL_${_install}DIR}")
    endif ()
    # If the path is still absolute, e.g., because the full install dirs did
    # were not subdirectories if the install prefix, give up and error. Nothing
    # we can do here.
    if (IS_ABSOLUTE "${CMAKE_INSTALL_${_install}DIR}")
      message(
        FATAL_ERROR
          "CMAKE_INSTALL_${_install}DIR must not be an absolute path for relocatable installations."
      )
    endif ()
  endforeach ()
  # For the docdir especially, lowercase the project name.
  if ("${CMAKE_INSTALL_DOCDIR}" STREQUAL
      "${CMAKE_INSTALL_DATAROOTDIR}/doc/${PROJECT_NAME}")
    string(TOLOWER "${PROJECT_NAME}" _name)
    set(CMAKE_INSTALL_DOCDIR "${CMAKE_INSTALL_DATAROOTDIR}/doc/${_name}")
    set(CMAKE_INSTALL_FULL_DOCDIR
        "${CMAKE_INSTALL_FULL_DATAROOTDIR}/doc/${_name}")
    unset(_name)
  endif ()
  unset(_install)
endmacro ()

# A replacement for target_link_libraries that links static libraries using
# the platform-specific whole-archive options. Please test any changes to this
# macro on all supported platforms and compilers.
macro (TenzirTargetLinkWholeArchive target visibility library)
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
      $<$<PLATFORM_ID:Windows>:LINKER:/WHOLEARCHIVE,$<TARGET_FILE:${library}>>)
  endif ()
  target_link_libraries(${target} ${visibility} ${library})
endmacro ()

# A utility macro for enabling tooling on a target.
macro (TenzirTargetEnableTooling _target)
  if (TENZIR_ENABLE_CLANG_TIDY)
    set_target_properties(
      "${_target}" PROPERTIES CXX_CLANG_TIDY "${TENZIR_CLANG_TIDY_ARGS}"
                              C_CLANG_TIDY "${TENZIR_CLANG_TIDY_ARGS}")
  endif ()
  if (TENZIR_ENABLE_CODE_COVERAGE)
    unset(_absolute_plugin_test_sources)
    foreach (plugin_test_source IN LISTS PLUGIN_TEST_SOURCES)
      if (IS_ABSOLUTE "${plugin_test_source}")
        list(APPEND _absolute_plugin_test_sources "${plugin_test_source}")
      else ()
        list(APPEND _absolute_plugin_test_sources
             "${CMAKE_CURRENT_SOURCE_DIR}/${plugin_test_source}")
      endif ()
    endforeach ()
    target_code_coverage(
      "${_target}"
      ${ARGV}
      EXCLUDE
      "${PROJECT_SOURCE_DIR}/libtenzir/aux/*"
      "${PROJECT_SOURCE_DIR}/libtenzir/test/*"
      "${PROJECT_SOURCE_DIR}/libtenzir_test/*"
      "${PROJECT_BINARY_DIR}/*"
      ${_absolute_plugin_test_sources})
  endif ()
endmacro ()

macro (make_absolute vars)
  list(GET "${vars}" 0 _glob)
  if ("${_glob}" STREQUAL "GLOB")
    list(REMOVE_AT "${vars}" 0)
    file(GLOB_RECURSE "${vars}" CONFIGURE_DEPENDS ${${vars}})
  endif ()
  unset(_glob)
  foreach (var IN LISTS "${vars}")
    get_filename_component(var_abs "${var}" ABSOLUTE)
    list(APPEND vars_abs "${var_abs}")
  endforeach ()
  set("${vars}" "${vars_abs}")
  unset(vars)
  unset(vars_abs)
endmacro ()

function (TenzirCompileFlatBuffers)
  cmake_parse_arguments(
    # <prefix>
    FBS
    # <options>
    ""
    # <one_value_keywords>
    "TARGET;INCLUDE_DIRECTORY"
    # <multi_value_keywords>
    "SCHEMAS"
    # <args>...
    ${ARGN})

  if (NOT FBS_TARGET)
    message(
      FATAL_ERROR "TARGET must be specified in call to TenzirCompileFlatBuffers"
    )
  elseif (TARGET ${FBS_TARGET})
    message(
      FATAL_ERROR
        "TARGET provided in call to TenzirCompileFlatBuffers already exists")
  endif ()

  if (NOT FBS_INCLUDE_DIRECTORY)
    message(
      FATAL_ERROR
        "INCLUDE_DIRECTORY must be specified in call to TenzirCompileFlatBuffers"
    )
  elseif (IS_ABSOLUTE FBS_INCLUDE_DIRECTORY)
    message(
      FATAL_ERROR
        "INCLUDE_DIRECTORY provided in a call to TenzirCompileFlatBuffers must be relative"
    )
  endif ()

  if (NOT FBS_SCHEMAS)
    message(
      FATAL_ERROR
        "SCHEMAS must be specified in call to TenzirCompileFlatBuffers")
  endif ()
  make_absolute(FBS_SCHEMAS)

  # An internal target for modeling inter-schema dependency.
  add_library(${FBS_TARGET} INTERFACE)

  # Link our target against FlatBuffers.
  find_package(Flatbuffers CONFIG)
  # Ugly workaround for Flatbuffers 2.0.8, which briefly changed the
  # project name to "FlatBuffers".
  if (NOT Flatbuffers_FOUND)
    find_package(FlatBuffers CONFIG)
    if (NOT FlatBuffers_FOUND)
      message(FATAL_ERROR "Flatbuffers is required but can't be found.")
    endif ()
  endif ()
  if (TARGET flatbuffers::flatbuffers)
    set(flatbuffers_target flatbuffers::flatbuffers)
  elseif (NOT TENZIR_ENABLE_STATIC_EXECUTABLE
          AND TARGET flatbuffers::flatbuffers_shared)
    set(flatbuffers_target flatbuffers::flatbuffers_shared)
  else ()
    message(
      FATAL_ERROR
        "Found FlatBuffers, but neither shared nor static library targets exist"
    )
  endif ()

  if ("${PROJECT_NAME}" STREQUAL "Tenzir")
    target_link_libraries(${FBS_TARGET} INTERFACE "${flatbuffers_target}")
    dependency_summary("FlatBuffers" ${flatbuffers_target} "Dependencies")
  endif ()

  set(output_prefix "${CMAKE_CURRENT_BINARY_DIR}/${FBS_TARGET}/include")
  # We include the generated files uisng -isystem because there's
  # nothing we can realistically do about the warnings in them, which
  # are especially annoying with clang-tidy enabled.
  target_include_directories(
    ${FBS_TARGET} SYSTEM
    INTERFACE $<BUILD_INTERFACE:${output_prefix}>
              $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

  if ("${CMAKE_PROJECT_NAME}" STREQUAL "Tenzir")
    set(FBS_PATH "${CMAKE_SOURCE_DIR}/libtenzir/fbs")
  else ()
    string(TOUPPER "${TENZIR_CMAKE_BUILD_TYPE}" build_type_uppercase_)
    get_target_property(FBS_PATH tenzir::tenzir
                        IMPORTED_LOCATION_${build_type_uppercase_})
    unset(build_type_uppercase_)
    get_filename_component(FBS_PATH "${FBS_PATH}" DIRECTORY)
    get_filename_component(FBS_PATH "${FBS_PATH}" DIRECTORY)
    set(FBS_PATH "${FBS_PATH}/include/tenzir/fbs")
  endif ()

  file(GLOB included_flatbuffers_schemas CONFIGURE_DEPENDS "${FBS_PATH}/*.fbs")
  list(APPEND FBS_SCHEMAS ${included_flatbuffers_schemas})
  list(REMOVE_DUPLICATES FBS_SCHEMAS)

  foreach (schema IN LISTS FBS_SCHEMAS)
    get_filename_component(basename ${schema} NAME_WE)
    # The hardcoded path that flatc generates.
    set(output_file
        "${output_prefix}/${FBS_INCLUDE_DIRECTORY}/${basename}_generated.h")
    # The path that we want.
    set(desired_file
        "${output_prefix}/${FBS_INCLUDE_DIRECTORY}/${basename}.hpp")
    # Hackish way to patch generated FlatBuffers schemas to support our naming.
    set("rename_${basename}"
        "${CMAKE_CURRENT_BINARY_DIR}/flatbuffers_strip_suffix_${basename}.cmake"
    )
    file(
      WRITE "${CMAKE_CURRENT_BINARY_DIR}/fbs-strip-suffix-${basename}.cmake"
      "file(READ \"${desired_file}\" include)\n"
      "string(REGEX REPLACE\n"
      "      \"([^\\n]+)_generated.h\\\"\"\n"
      "      \"\\\\1.hpp\\\"\"\n"
      "      new_include \"\${include}\")\n"
      "file(WRITE \"${desired_file}\" \"\${new_include}\")\n")
    # Compile and rename schema.
    add_custom_command(
      OUTPUT "${desired_file}"
      COMMAND
        flatbuffers::flatc
        # Generate wire format binaries for any data definitions.
        --binary
        # Generate C++ headers using the C++17 standard.
        --cpp --cpp-std c++17 --scoped-enums
        # Generate type name functions.
        --gen-name-strings
        # Generate mutator functions.
        --gen-mutable
        # Allow including other files.
        -I "${FBS_PATH}"
        # Set output directory and schema inputs.
        -o "${output_prefix}/${FBS_INCLUDE_DIRECTORY}" "${schema}"
      COMMAND ${CMAKE_COMMAND} -E rename "${output_file}" "${desired_file}"
      COMMAND ${CMAKE_COMMAND} -P
              "${CMAKE_CURRENT_BINARY_DIR}/fbs-strip-suffix-${basename}.cmake"
      # We need to depend on all schemas here instead of just the one we're
      # compiling currently because schemas may include other schema files.
      DEPENDS ${FBS_SCHEMAS}
      COMMENT "Compiling FlatBuffers schema ${schema}")
    # We need an additional indirection via add_custom_target here, because
    # FBS_TARGET cannot depend on the OUTPUT of a add_custom_command directly,
    # and depending on a BYPRODUCT of a add_custom_command causes the Unix
    # Makefiles generator to constantly rebuild targets in the same export set.
    add_custom_target("compile-flatbuffers-schema-${FBS_TARGET}-${basename}"
                      DEPENDS "${desired_file}")
    add_dependencies(${FBS_TARGET}
                     "compile-flatbuffers-schema-${FBS_TARGET}-${basename}")
    set_property(
      TARGET ${FBS_TARGET}
      APPEND
      PROPERTY PUBLIC_HEADER "${desired_file}" "${schema}")
  endforeach ()

  install(
    TARGETS ${FBS_TARGET}
    EXPORT TenzirTargets
    PUBLIC_HEADER
      DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${FBS_INCLUDE_DIRECTORY}"
      COMPONENT Development)
endfunction ()

# Install a commented-out version of an example configuration file.
macro (TenzirInstallExampleConfiguration target source prefix destination)
  # Sanity checks macro inputs.
  if (NOT TARGET "${target}")
    message(FATAL_ERROR "target '${target}' does not exist")
  endif ()
  if (NOT IS_ABSOLUTE "${source}")
    message(FATAL_ERROR "source '${source}' must be absolute")
  endif ()
  if (prefix AND IS_ABSOLUTE "${prefix}")
    message(FATAL_ERROR "prefix '${prefix}' must be relative")
  endif ()
  if (IS_ABSOLUTE "${destination}")
    message(FATAL_ERROR "destination '${destination}' must be relative")
  endif ()

  # Provides install directory variables as defined for GNU software:
  # http://www.gnu.org/prep/standards/html_node/Directory-Variables.html
  include(GNUInstallDirs)
  if (TENZIR_ENABLE_RELOCATABLE_INSTALLATIONS)
    TenzirNormalizeInstallDirs()
  endif ()

  # Set a temporary variable for the example dir location. Because we're in a
  # macro we're unsetting the variable again later on.
  set(_example_dir "${CMAKE_INSTALL_DOCDIR}/examples")

  # Write a CMake file that does the desired text transformations.
  file(
    WRITE "${CMAKE_CURRENT_BINARY_DIR}/${target}-example-config.cmake"
    "\
    cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)
    file(READ \"${source}\" content)
    # Randomly generated string that temporarily replaces semicolons.
    set(dummy \"J.3t26kvfjEoi9BXbf2j.qMY\")
    string(REPLACE \";\" \"\${dummy}\" content \"\${content}\")
    string(REPLACE \"\\n\" \";\" content \"\${content}\")
    list(TRANSFORM content PREPEND \"#\")
    string(REPLACE \";\" \"\\n\" content \"\${content}\")
    string(REPLACE \"\${dummy}\" \";\" content \"\${content}\")
    file(WRITE
      \"${CMAKE_BINARY_DIR}/${_example_dir}/${prefix}${destination}\"
      \"# NOTE: For this file to take effect, move it to:\\n\"
      \"#   <prefix>/${CMAKE_INSTALL_SYSCONFDIR}/tenzir/${prefix}${destination}\\n\"
      \"\\n\"
      \"\${content}\")")

  add_custom_command(
    OUTPUT "${CMAKE_BINARY_DIR}/${_example_dir}/${prefix}${destination}"
    MAIN_DEPENDENCY "${source}"
    COMMENT "Copying example configuration file ${source}"
    COMMAND ${CMAKE_COMMAND} -P
            "${CMAKE_CURRENT_BINARY_DIR}/${target}-example-config.cmake")

  add_custom_target(
    ${target}-copy-example-configuration-file
    DEPENDS "${CMAKE_BINARY_DIR}/${_example_dir}/${prefix}${destination}")

  add_dependencies(${target} ${target}-copy-example-configuration-file)

  install(
    FILES "${CMAKE_BINARY_DIR}/${_example_dir}/${prefix}${destination}"
    DESTINATION "${_example_dir}/${prefix}"
    COMPONENT Runtime)

  unset(_example_dir)
endmacro ()

# Support tools like clang-tidy by creating a compilation database and copying
# it to the project root.
function (TenzirExportCompileCommands)
  if (TARGET compilation-database)
    message(FATAL_ERROR "TenzirExportCompileCommands must be called only once")
  endif ()

  if (NOT ${ARGC} EQUAL 1)
    message(
      FATAL_ERROR "TenzirExportCompileCommands takes exactly one argument")
  endif ()

  if (NOT TARGET ${ARGV0})
    message(FATAL_ERROR "TenzirExportCompileCommands provided invalid target")
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

function (TenzirRegisterPlugin)
  cmake_parse_arguments(
    # <prefix>
    PLUGIN
    # <options>
    ""
    # <one_value_keywords>
    "TARGET;ENTRYPOINT"
    # <multi_value_keywords>
    "SOURCES;TEST_SOURCES;INCLUDE_DIRECTORIES;DEPENDENCIES;BUILTINS;FLATBUFFERS"
    # <args>...
    ${ARGN})

  # Safeguard against repeated TenzirRegisterPlugin calls from the same project.
  # While technically possible for external pluigins, doing so makes it
  # impossible to build the plugin alongside Tenzir.
  get_property(TENZIR_PLUGIN_PROJECT_SOURCE_DIRS GLOBAL
               PROPERTY "TENZIR_PLUGIN_PROJECT_SOURCE_DIRS_PROPERTY")
  if ("${PROJECT_SOURCE_DIR}" IN_LIST TENZIR_PLUGIN_PROJECT_SOURCE_DIRS)
    message(
      FATAL_ERROR
        "TenzirRegisterPlugin called twice in CMake project ${PROJECT_NAME}")
  endif ()
  set_property(
    GLOBAL APPEND PROPERTY "TENZIR_PLUGIN_PROJECT_SOURCE_DIRS_PROPERTY"
                           "${PROJECT_SOURCE_DIR}")

  # Set a default build type if none was specified.
  if (NOT "${CMAKE_PROJECT_NAME}" STREQUAL "Tenzir")
    if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
      set(CMAKE_BUILD_TYPE
          "${TENZIR_CMAKE_BUILD_TYPE}"
          CACHE STRING "Choose the type of build." FORCE)
      set(CMAKE_CONFIGURATION_TYPES
          "${TENZIR_CMAKE_CONFIGURATION_TYPES}"
          CACHE STRING
                "Choose the types of builds for multi-config generators." FORCE)
      # Set the possible values of build type for cmake-gui.
      set_property(
        CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel"
                                        "RelWithDebInfo")
    endif ()
  endif ()

  # Provides install directory variables as defined for GNU software:
  # http://www.gnu.org/prep/standards/html_node/Directory-Variables.html
  include(GNUInstallDirs)
  if (TENZIR_ENABLE_RELOCATABLE_INSTALLATIONS)
    TenzirNormalizeInstallDirs()
  endif ()

  # Enable compile commands for external plugins.
  # Must be done before the targets are created for CMake >= 3.20.
  # TODO: Replace this with the corresponding interface target property on
  # libtenzir_internal when bumping the minimum CMake version to 3.20.
  if (NOT "${CMAKE_PROJECT_NAME}" STREQUAL "Tenzir")
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
  endif ()

  if (NOT PLUGIN_TARGET)
    message(
      FATAL_ERROR "TARGET must be specified in call to TenzirRegisterPlugin")
  endif ()

  if (NOT PLUGIN_ENTRYPOINT)
    list(LENGTH PLUGIN_SOURCES num_sources)
    if (num_sources EQUAL 1)
      list(GET PLUGIN_SOURCES 0 PLUGIN_ENTRYPOINT)
      set(PLUGIN_SOURCES "")
    else ()
      message(
        FATAL_ERROR
          "ENTRYPOINT must be specified in call to TenzirRegisterPlugin")
    endif ()
  endif ()

  # Make all given paths absolute.
  make_absolute(PLUGIN_ENTRYPOINT)
  make_absolute(PLUGIN_SOURCES)
  make_absolute(PLUGIN_TEST_SOURCES)
  make_absolute(PLUGIN_INCLUDE_DIRECTORIES)
  make_absolute(PLUGIN_BUILTINS)
  make_absolute(PLUGIN_FLATBUFFERS)

  # Set up builtins bundled with the plugin.
  if (PLUGIN_BUILTINS)
    foreach (builtin IN LISTS PLUGIN_BUILTINS)
      file(READ "${builtin}" lines)
      if (NOT "${lines}" MATCHES "\n *TENZIR_REGISTER_PLUGIN *\\(.+\\) *[\n$]")
        message(FATAL_ERROR "builtin ${builtin} does not register as a plugin")
      endif ()
      set_property(
        SOURCE "${builtin}"
        APPEND
        PROPERTY COMPILE_DEFINITIONS "TENZIR_ENABLE_BUILTINS=1"
                 "TENZIR_BUILTIN_DEPENDENCY=${PLUGIN_TARGET}")
    endforeach ()

    list(APPEND PLUGIN_SOURCES ${PLUGIN_BUILTINS})
    list(REMOVE_DUPLICATES PLUGIN_SOURCES)
  endif ()

  # Deduplicate the entrypoint so plugin authors can grep for sources while
  # still specifying the entrypoint manually.
  if ("${PLUGIN_ENTRYPOINT}" IN_LIST PLUGIN_SOURCES)
    list(REMOVE_ITEM PLUGIN_SOURCES "${PLUGIN_ENTRYPOINT}")
  endif ()

  # Create an object library target for our plugin _without_ the entrypoint.
  file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/stub.h" "")
  list(APPEND PLUGIN_SOURCES "${CMAKE_CURRENT_BINARY_DIR}/stub.h")
  add_library(${PLUGIN_TARGET} OBJECT ${PLUGIN_SOURCES})
  # NOTE: CMake isn't able to deduce this automatically. Usually, this happens
  # when there are no sources for target, but since the plugins have sources, we
  # manually specify this here. `CXX` because we only use C/C++ currently.
  set_target_properties(${PLUGIN_TARGET} PROPERTIES LINKER_LANGUAGE CXX)
  TenzirTargetEnableTooling(${PLUGIN_TARGET})
  target_link_libraries(
    ${PLUGIN_TARGET}
    PUBLIC tenzir::libtenzir
    PRIVATE tenzir::internal)

  set(PLUGIN_OUTPUT_DIRECTORY
      "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR}/tenzir/plugins")

  if (NOT "${CMAKE_PROJECT_NAME}" STREQUAL "Tenzir")
    string(TOUPPER "${TENZIR_CMAKE_BUILD_TYPE}" build_type_uppercase_)
    get_target_property(TENZIR_BINARY tenzir::tenzir
                        IMPORTED_LOCATION_${build_type_uppercase_})
    unset(build_type_uppercase_)
    message(STATUS "Found tenzir executable: ${TENZIR_BINARY}")
    get_filename_component(TENZIR_BINARY_DIR "${TENZIR_BINARY}" DIRECTORY)
    file(
      WRITE "${CMAKE_CURRENT_BINARY_DIR}/make_plugin_wrapper.cmake"
      "\
      file(WRITE \"${CMAKE_CURRENT_BINARY_DIR}/bin/\${EXECUTABLE}\"
      \"\\
      #!/bin/sh

      export TENZIR_PLUGIN_DIRS=\\\${TENZIR_PLUGIN_DIRS:+\\\${TENZIR_PLUGIN_DIRS}:}${PLUGIN_OUTPUT_DIRECTORY}
      exec \\\"${TENZIR_BINARY_DIR}/\${EXECUTABLE}\\\" \\\"\\\$@\\\"\")
      file(CHMOD \"${CMAKE_CURRENT_BINARY_DIR}/bin/\${EXECUTABLE}\"
        FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                         GROUP_READ GROUP_WRITE GROUP_EXECUTE)")
    add_custom_target(
      ${PLUGIN_TARGET}-wrapper ALL
      COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_BINARY_DIR}/bin"
      COMMAND ${CMAKE_COMMAND} -D EXECUTABLE=tenzir -P
              "${CMAKE_CURRENT_BINARY_DIR}/make_plugin_wrapper.cmake"
      COMMAND ${CMAKE_COMMAND} -D EXECUTABLE=tenzir-ctl -P
              "${CMAKE_CURRENT_BINARY_DIR}/make_plugin_wrapper.cmake"
      COMMAND ${CMAKE_COMMAND} -D EXECUTABLE=tenzir-node -P
              "${CMAKE_CURRENT_BINARY_DIR}/make_plugin_wrapper.cmake"
      COMMAND ${CMAKE_COMMAND} -D EXECUTABLE=tenzir -P
              "${CMAKE_CURRENT_BINARY_DIR}/make_plugin_wrapper.cmake"
      COMMENT
        "Creating convenience binary wrappers in ${CMAKE_CURRENT_BINARY_DIR}/bin"
    )
  endif ()

  # Set up the target's include directories.
  if (PLUGIN_INCLUDE_DIRECTORIES)
    # We intentionally omit the install interface include directories and do not
    # install the given include directories, as installed plugin libraries are
    # not meant to be developed against.
    list(JOIN PLUGIN_INCLUDE_DIRECTORIES " " include_directories)
    target_include_directories(${PLUGIN_TARGET}
                               PUBLIC $<BUILD_INTERFACE:${include_directories}>)
  endif ()

  string(MAKE_C_IDENTIFIER "tenzir_plugin_${PLUGIN_TARGET}_version"
                           PLUGIN_TARGET_IDENTIFIER)
  if (NOT PROJECT_VERSION)
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/config.cpp"
         "const char* ${PLUGIN_TARGET_IDENTIFIER} = nullptr;\n")
  else ()
    # Require that the CMake project version has the appropriate format.
    if (NOT PROJECT_VERSION MATCHES "^[0-9]+\\.[0-9]+\\.[0-9]+$")
      message(
        FATAL_ERROR
          "PROJECT_VERSION does not match expected format: <major>.<minor>.<patch>"
      )
    endif ()

    # Determine the plugin version. We use the CMake project version, and then
    # optionally append the Git revision that last touched the project,
    # essentially reconstructing git-describe except for the revision count.
    file(
      WRITE "${CMAKE_CURRENT_BINARY_DIR}/config.cpp.in"
      "const char* ${PLUGIN_TARGET_IDENTIFIER} = \"@TENZIR_PLUGIN_VERSION@\";\n"
    )
    string(TOUPPER "TENZIR_PLUGIN_${PLUGIN_TARGET}_REVISION"
                   TENZIR_PLUGIN_REVISION_VAR)
    if (DEFINED "${TENZIR_PLUGIN_REVISION_VAR}")
      set(TENZIR_PLUGIN_REVISION_FALLBACK "${${TENZIR_PLUGIN_REVISION_VAR}}")
    endif ()
    file(
      WRITE "${CMAKE_CURRENT_BINARY_DIR}/update-config.cmake"
      "\
      find_package(Git QUIET)
      if (Git_FOUND)
        execute_process(
          COMMAND \"\${GIT_EXECUTABLE}\" -C \"${PROJECT_SOURCE_DIR}\" rev-list
                  --abbrev-commit --abbrev=10 -1 HEAD -- \"${PROJECT_SOURCE_DIR}\"
          OUTPUT_VARIABLE PLUGIN_REVISION
          OUTPUT_STRIP_TRAILING_WHITESPACE
          RESULT_VARIABLE PLUGIN_REVISION_RESULT
	  ERROR_QUIET)
        if (PLUGIN_REVISION_RESULT EQUAL 0)
          string(PREPEND PLUGIN_REVISION \"g\")
          execute_process(
            COMMAND \"\${GIT_EXECUTABLE}\" -C \"${PROJECT_SOURCE_DIR}\" diff-index
                    --quiet HEAD -- \"${PROJECT_SOURCE_DIR}\"
            RESULT_VARIABLE PLUGIN_DIRTY_RESULT)
          if (NOT PLUGIN_DIRTY_RESULT EQUAL 0)
            string(APPEND PLUGIN_REVISION \"-dirty\")
          endif ()
        endif ()
      endif ()
      if (NOT PLUGIN_REVISION)
        set(PLUGIN_REVISION \"${TENZIR_PLUGIN_REVISION_FALLBACK}\")
      endif ()
      set(TENZIR_PLUGIN_VERSION \"v${PROJECT_VERSION}\")
      if (PLUGIN_REVISION)
        string(APPEND TENZIR_PLUGIN_VERSION \"-\${PLUGIN_REVISION}\")
      endif ()
      configure_file(\"${CMAKE_CURRENT_BINARY_DIR}/config.cpp.in\"
                     \"${CMAKE_CURRENT_BINARY_DIR}/config.cpp\" @ONLY)")
    add_custom_target(
      ${PLUGIN_TARGET}-update-config
      BYPRODUCTS "${CMAKE_CURRENT_BINARY_DIR}/config.cpp"
      COMMAND ${CMAKE_COMMAND} -P
              "${CMAKE_CURRENT_BINARY_DIR}/update-config.cmake")
  endif ()

  unset(formatted_dependencies)
  foreach (dependency IN LISTS PLUGIN_DEPENDENCIES)
    list(APPEND formatted_dependencies "\"${dependency}\"")
  endforeach ()
  list(JOIN formatted_dependencies "," formatted_dependencies)
  set_property(
    SOURCE "${PLUGIN_ENTRYPOINT}"
    APPEND
    PROPERTY COMPILE_DEFINITIONS
             "TENZIR_PLUGIN_VERSION=${PLUGIN_TARGET_IDENTIFIER}"
             "TENZIR_PLUGIN_DEPENDENCIES=${formatted_dependencies}")
  unset(formatted_dependencies)

  list(APPEND PLUGIN_ENTRYPOINT "${CMAKE_CURRENT_BINARY_DIR}/config.cpp")

  # Create a static library target for our plugin with the entrypoint, and use
  # static versions of TENZIR_REGISTER_PLUGIN family of macros.
  add_library(${PLUGIN_TARGET}-static STATIC ${PLUGIN_ENTRYPOINT})
  TenzirTargetEnableTooling(${PLUGIN_TARGET}-static)
  TenzirTargetLinkWholeArchive(${PLUGIN_TARGET}-static PUBLIC ${PLUGIN_TARGET})
  target_link_libraries(${PLUGIN_TARGET}-static PRIVATE tenzir::internal)
  target_compile_definitions(${PLUGIN_TARGET}-static
                             PRIVATE TENZIR_ENABLE_STATIC_PLUGINS)

  if (TENZIR_ENABLE_STATIC_PLUGINS)
    # Link our static library against the tenzir binary directly.
    TenzirTargetLinkWholeArchive(tenzir PRIVATE ${PLUGIN_TARGET}-static)
  else ()
    # Override BUILD_SHARED_LIBS to force add_library to do the correct thing
    # depending on the plugin type. This must not be user-configurable for
    # plugins, as building external plugins should work without needing to enable
    # this explicitly.
    set(BUILD_SHARED_LIBS
        ON
        PARENT_SCOPE)

    # Enable position-independent code for the static library if we're linking
    # it into shared one.
    set_property(TARGET ${PLUGIN_TARGET} PROPERTY POSITION_INDEPENDENT_CODE ON)

    # Create a shared library target for our plugin.
    add_library(${PLUGIN_TARGET}-shared SHARED ${PLUGIN_ENTRYPOINT})
    TenzirTargetEnableTooling(${PLUGIN_TARGET}-shared)
    TenzirTargetLinkWholeArchive(${PLUGIN_TARGET}-shared PUBLIC
                                 ${PLUGIN_TARGET})
    target_link_libraries(${PLUGIN_TARGET}-shared PRIVATE tenzir::internal)

    # Install the plugin library to <libdir>/tenzir/plugins, and also configure
    # the library output directory accordingly.
    set_target_properties(
      ${PLUGIN_TARGET}-shared
      PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${PLUGIN_OUTPUT_DIRECTORY}"
                 OUTPUT_NAME "tenzir-plugin-${PLUGIN_TARGET}")
    install(
      TARGETS ${PLUGIN_TARGET}-shared
      DESTINATION "${CMAKE_INSTALL_LIBDIR}/tenzir/plugins"
      COMPONENT Runtime)

    # Ensure that Tenzir only runs after all dynamic plugin libraries are built.
    if (TARGET tenzir)
      add_dependencies(tenzir ${PLUGIN_TARGET}-shared)
    endif ()
  endif ()

  if (PLUGIN_FLATBUFFERS)
    TenzirCompileFlatBuffers(
      TARGET ${PLUGIN_TARGET}-fbs
      SCHEMAS ${PLUGIN_FLATBUFFERS}
      INCLUDE_DIRECTORY "${PLUGIN_TARGET}/fbs")
    target_link_libraries(${PLUGIN_TARGET} PUBLIC ${PLUGIN_TARGET}-fbs)
  endif ()

  # Install an example configuration file, if it exists at the plugin project root.
  if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${PLUGIN_TARGET}.yaml.example")
    TenzirInstallExampleConfiguration(
      ${PLUGIN_TARGET}
      "${CMAKE_CURRENT_SOURCE_DIR}/${PLUGIN_TARGET}.yaml.example"
      "plugin/${PLUGIN_TARGET}/" "${PLUGIN_TARGET}.yaml")
  endif ()

  if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/schema")
    # Install the bundled schema files to <datadir>/tenzir.
    install(
      DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/schema"
      DESTINATION "${CMAKE_INSTALL_DATADIR}/tenzir/plugin/${PLUGIN_TARGET}"
      COMPONENT Runtime)
    if (TENZIR_ENABLE_RELOCATABLE_INSTALLATIONS)
      # Copy schemas from bundled plugins to the build directory so they can be
      # used from a Tenzir in a build directory (instead if just an installed Tenzir).
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
        set(plugin_schema_dir
            "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_DATADIR}/tenzir/plugin/${PLUGIN_TARGET}/schema"
        )
        add_custom_target(
          tenzir-schema-${plugin_schema_file_hash}
          BYPRODUCTS "${plugin_schema_dir}/${relative_plugin_schema_file}"
          COMMAND "${CMAKE_COMMAND}" -E copy "${plugin_schema_file}"
                  "${plugin_schema_dir}/${relative_plugin_schema_file}"
          COMMENT
            "Copying schema file ${relative_plugin_schema_file} for plugin ${PLUGIN_TARGET}"
        )
        add_dependencies(${PLUGIN_TARGET}
                         tenzir-schema-${plugin_schema_file_hash})
      endforeach ()
    endif ()
  endif ()

  # Setup unit tests.
  if (TENZIR_ENABLE_UNIT_TESTS AND PLUGIN_TEST_SOURCES)
    set(TENZIR_UNIT_TEST_TIMEOUT
        "60"
        CACHE STRING "The per-test timeout in unit tests" FORCE)
    unset(suites)
    foreach (test_source IN LISTS PLUGIN_TEST_SOURCES)
      get_filename_component(suite "${test_source}" NAME_WE)
      set_property(SOURCE "${test_source}" PROPERTY COMPILE_DEFINITIONS
                                                    "SUITE=${suite}")
      list(APPEND suites "${suite}")
    endforeach ()
    add_executable(${PLUGIN_TARGET}-test ${PLUGIN_TEST_SOURCES})
    TenzirTargetEnableTooling(${PLUGIN_TARGET}-test)
    target_link_libraries(${PLUGIN_TARGET}-test PRIVATE tenzir::test
                                                        tenzir::internal
                                                        tenzir::libtenzir_builtins)
    TenzirTargetLinkWholeArchive(${PLUGIN_TARGET}-test PRIVATE
                                 ${PLUGIN_TARGET}-static)
    add_test(NAME build-${PLUGIN_TARGET}-test
             COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --config
                     "$<CONFIG>" --target ${PLUGIN_TARGET}-test)
    set_tests_properties(
      build-${PLUGIN_TARGET}-test
      PROPERTIES FIXTURES_SETUP tenzir_${PLUGIN_TARGET}_unit_test_fixture
                 FIXTURES_REQUIRED tenzir_unit_test_fixture)
    foreach (suite IN LISTS suites)
      string(REPLACE " " "_" test_name ${suite})
      add_test(NAME "plugin/${PLUGIN_TARGET}/${test_name}"
               COMMAND ${PLUGIN_TARGET}-test -v 4 -r
                       "${TENZIR_UNIT_TEST_TIMEOUT}" -s "${suite}")
      set_tests_properties(
        "plugin/${PLUGIN_TARGET}/${test_name}"
        PROPERTIES FIXTURES_REQUIRED tenzir_${PLUGIN_TARGET}_unit_test_fixture)
    endforeach ()
  endif ()

  # Ensure that a target integration always exists, even if a plugin does not
  # define integration tests.
  if (NOT TARGET integration)
    add_custom_target(integration)
    TenzirDefineUpdateIntegrationTarget(
      integration "${CMAKE_CURRENT_SOURCE_DIR}/integration/data/reference")
  endif ()

  # Setup integration tests.
  if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/integration/tests")
    if ("${CMAKE_PROJECT_NAME}" STREQUAL "Tenzir")
      set(TENZIR_PATH "$<TARGET_FILE_DIR:tenzir::tenzir>")
    else ()
      file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/share/tenzir")
      file(CREATE_LINK "${TENZIR_PREFIX_DIR}/share/tenzir/integration"
           "${CMAKE_BINARY_DIR}/share/tenzir/integration" SYMBOLIC)
      set(TENZIR_PATH "${CMAKE_CURRENT_BINARY_DIR}/bin")
    endif ()
    include(ProcessorCount)
    ProcessorCount(parallel_level)
    math(EXPR parallel_level "${parallel_level} + 2")
    file(GLOB_RECURSE _suites CONFIGURE_DEPENDS
         "${CMAKE_CURRENT_SOURCE_DIR}/integration/tests/*.bats")
    add_custom_target(integration-${PLUGIN_TARGET})
    add_dependencies(integration integration-${PLUGIN_TARGET})
    TenzirDefineUpdateIntegrationTarget(
      integration-${PLUGIN_TARGET}
      "${CMAKE_CURRENT_SOURCE_DIR}/integration/data/reference")
    foreach (suite IN LISTS _suites)
      get_filename_component(bats_suite_name "${suite}" NAME_WE)
      string(REGEX REPLACE "_" "-" suite_name "${bats_suite_name}")
      add_custom_target(
        integration-${PLUGIN_TARGET}-${suite_name}
        COMMAND
          ${CMAKE_COMMAND} -E env
          PATH="${TENZIR_PATH}:\$\$PATH:${TENZIR_PATH}/../share/tenzir/integration/lib/bats/bin"
          bats "-r" "-T" "--jobs" "${parallel_level}" "${suite}"
        COMMENT
          "Executing ${PLUGIN_TARGET} integration test suite ${suite_name}..."
        USES_TERMINAL)
      add_dependencies(integration-${PLUGIN_TARGET}-${suite_name}
                       tenzir::tenzir)
      add_dependencies(integration-${PLUGIN_TARGET}
                       integration-${PLUGIN_TARGET}-${suite_name})
      TenzirDefineUpdateIntegrationTarget(
        integration-${PLUGIN_TARGET}-${suite_name}
        "${CMAKE_CURRENT_SOURCE_DIR}/integration/data/reference/${bats_suite_name}"
      )
    endforeach ()
    unset(parallel_level)
    unset(TENZIR_PATH)
  endif ()

  if ("${CMAKE_PROJECT_NAME}" STREQUAL "Tenzir")
    # Provide niceties for building alongside Tenzir.
    dependency_summary("${PLUGIN_TARGET}" "${CMAKE_CURRENT_LIST_DIR}" "Plugins")
    set_property(GLOBAL APPEND PROPERTY "TENZIR_BUNDLED_PLUGINS_PROPERTY"
                                        "${PLUGIN_TARGET}")
  else ()
    # Provide niceties for external plugins that are usually part of Tenzir.
    TenzirExportCompileCommands(${PLUGIN_TARGET})
  endif ()
endfunction ()

# Helper utility for printing the status of dependencies.
function (dependency_summary name what category)
  get_property(TENZIR_DEPENDENCY_SUMMARY_CATEGORIES GLOBAL
               PROPERTY "TENZIR_DEPENDENCY_SUMMARY_CATEGORIES_PROPERTY")
  if (NOT "${category}" IN_LIST TENZIR_DEPENDENCY_SUMMARY_CATEGORIES)
    list(APPEND TENZIR_DEPENDENCY_SUMMARY_CATEGORIES "${category}")
    set_property(GLOBAL PROPERTY "TENZIR_DEPENDENCY_SUMMARY_CATEGORIES_PROPERTY"
                                 "${TENZIR_DEPENDENCY_SUMMARY_CATEGORIES}")
  endif ()
  get_property(TENZIR_DEPENDENCY_SUMMARY GLOBAL
               PROPERTY "TENZIR_DEPENDENCY_SUMMARY_${category}_PROPERTY")
  if (TARGET ${what})
    get_target_property(type "${what}" TYPE)
    if (type STREQUAL "INTERFACE_LIBRARY")
      get_target_property(location "${what}" INTERFACE_INCLUDE_DIRECTORIES)
      if (EXISTS "${location}")
        get_filename_component(location "${location}" DIRECTORY)
      endif ()
    else ()
      get_target_property(location "${what}" LOCATION)
      if (EXISTS "${location}")
        get_filename_component(location "${location}" DIRECTORY)
        get_filename_component(location "${location}" DIRECTORY)
      endif ()
    endif ()
  else ()
    set(location "${what}")
  endif ()
  string(GENEX_STRIP "${location}" location)
  if ("${location}" STREQUAL "")
    set(location "Unknown")
  elseif ("${location}" MATCHES "-NOTFOUND$")
    set(location "Not found")
  endif ()
  list(APPEND TENZIR_DEPENDENCY_SUMMARY " * ${name}: ${location}")
  list(SORT TENZIR_DEPENDENCY_SUMMARY)
  list(REMOVE_DUPLICATES TENZIR_DEPENDENCY_SUMMARY)
  set_property(GLOBAL PROPERTY "TENZIR_DEPENDENCY_SUMMARY_${category}_PROPERTY"
                               "${TENZIR_DEPENDENCY_SUMMARY}")
endfunction ()

function (TenzirPrintSummary)
  get_directory_property(_is_subproject PARENT_DIRECTORY)
  if (NOT _is_subproject)
    feature_summary(WHAT ENABLED_FEATURES DISABLED_FEATURES
                    INCLUDE_QUIET_PACKAGES)
    unset(build_summary)
    # In case we're building a plugin.
    if (TENZIR_PREFIX_DIR)
      list(APPEND build_summary " * Tenzir Prefix: ${TENZIR_PREFIX_DIR}")
    endif ()
    if (TENZIR_EDITION_NAME)
      list(APPEND build_summary " * Edition: ${TENZIR_EDITION_NAME}")
    endif ()
    list(APPEND build_summary " * Version: ${TENZIR_VERSION_FULL}")
    if (TENZIR_BUILD_TREE_HASH)
      list(APPEND build_summary " * Build Tree Hash: ${TENZIR_BUILD_TREE_HASH}")
    endif ()
    if (build_summary)
      list(APPEND build_summary "")
    endif ()
    if (CMAKE_CONFIGURATION_TYPES)
      list(APPEND build_summary
           " * Configuration Types: ${CMAKE_CONFIGURATION_TYPES}")
    else ()
      list(APPEND build_summary " * Build Type: ${CMAKE_BUILD_TYPE}")
    endif ()
    list(APPEND build_summary " * Source Directory: ${CMAKE_SOURCE_DIR}")
    list(APPEND build_summary " * Binary Directory: ${CMAKE_BINARY_DIR}\n")
    list(APPEND build_summary " * System Name: ${CMAKE_SYSTEM_NAME}")
    list(APPEND build_summary " * CPU: ${CMAKE_SYSTEM_PROCESSOR}")
    foreach (lang IN ITEMS C CXX)
      set(_lang_compiler "${CMAKE_${lang}_COMPILER}")
      set(_lang_compiler_info
          "${CMAKE_${lang}_COMPILER_ID} ${CMAKE_${lang}_COMPILER_VERSION}")
      set(_lang_flags
          "${CMAKE_${lang}_FLAGS} ${CMAKE_${lang}_FLAGS_${CMAKE_BUILD_TYPE}}
        ${CMAKE_CPP_FLAGS} ${CMAKE_CPP_FLAGS_${CMAKE_BUILD_TYPE}}")
      string(STRIP "${_lang_flags}" _lang_flags)
      if (_lang_flags)
        list(
          APPEND
          build_summary
          " * ${lang} Compiler: ${_lang_compiler} (${_lang_compiler_info} with ${_lang_flags})"
        )
      else ()
        list(APPEND build_summary
             " * ${lang} Compiler: ${_lang_compiler} (${_lang_compiler_info})")
      endif ()
    endforeach ()
    list(APPEND build_summary " * Linker: ${CMAKE_LINKER}")
    list(APPEND build_summary " * Archiver: ${CMAKE_AR}")
    list(APPEND build_summary "")
    list(PREPEND build_summary "Build summary:\n")
    list(JOIN build_summary "\n" build_summary_joined)
    message(STATUS "${build_summary_joined}")
    get_property(TENZIR_DEPENDENCY_SUMMARY_CATEGORIES GLOBAL
                 PROPERTY "TENZIR_DEPENDENCY_SUMMARY_CATEGORIES_PROPERTY")
    foreach (category IN LISTS TENZIR_DEPENDENCY_SUMMARY_CATEGORIES)
      get_property(TENZIR_DEPENDENCY_SUMMARY GLOBAL
                   PROPERTY "TENZIR_DEPENDENCY_SUMMARY_${category}_PROPERTY")
      if (TENZIR_DEPENDENCY_SUMMARY)
        unset(build_summary)
        list(APPEND build_summary "${category}:")
        foreach (summary IN LISTS TENZIR_DEPENDENCY_SUMMARY)
          list(APPEND build_summary "${summary}")
        endforeach ()
        list(APPEND build_summary "")
        list(JOIN build_summary "\n" build_summary_joined)
        message(STATUS "${build_summary_joined}")
      endif ()
    endforeach ()
  endif ()
endfunction ()

function (TenzirOnFileChange variable access value current stack)
  # Print the feature and build summary if we're not a subproject.
  if ("${current}" STREQUAL "")
    tenzirprintsummary()
  endif ()
endfunction ()
variable_watch(CMAKE_CURRENT_LIST_DIR TenzirOnFileChange)
