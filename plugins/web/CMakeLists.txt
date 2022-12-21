cmake_minimum_required(VERSION 3.18...3.24 FATAL_ERROR)

project(
  web
  VERSION 1.0.0
  DESCRIPTION "Web plugin for VAST"
  LANGUAGES CXX)

include(FeatureSummary)
include(CTest)

file(GLOB_RECURSE web_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
     "${CMAKE_CURRENT_SOURCE_DIR}/include/web/*.hpp")

file(GLOB_RECURSE web_tests CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")

find_package(VAST REQUIRED)

VASTRegisterPlugin(
  TARGET web
  ENTRYPOINT src/plugin.cpp
  SOURCES ${web_sources}
  TEST_SOURCES ${web_tests}
  INCLUDE_DIRECTORIES include)

find_package(OpenSSL REQUIRED)

option(VAST_ENABLE_BUNDLED_AND_PATCHED_RESTINIO
       "Always use the bundled and patched restinio" ON)
add_feature_info(
  "VAST_ENABLE_BUNDLED_AND_PATCHED_RESTINIO"
  VAST_ENABLE_BUNDLED_AND_PATCHED_RESTINIO
  "always use the bundled and patched restinio.")
if (NOT VAST_ENABLE_BUNDLED_AND_PATCHED_RESTINIO)
  find_package(fmt REQUIRED)
  find_package(restinio REQUIRED)
  # OpenSSL link is missing from restinio.
  find_package(OpenSSL REQUIRED)
  target_link_libraries(web PUBLIC OpenSSL::SSL OpenSSL::Crypto)
else ()
  # Setup dependency code from submodules.
  list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
  add_subdirectory(aux)
  list(POP_FRONT CMAKE_MODULE_PATH)
endif ()
target_link_libraries(web PRIVATE restinio::restinio)

# Compile the web FlatBuffers schemas.
file(GLOB flatbuffers_schemas CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/fbs/*.fbs")

VASTCompileFlatBuffers(
  TARGET web-fbs
  SCHEMAS ${flatbuffers_schemas}
  INCLUDE_DIRECTORY "web/fbs")

target_link_libraries(web PUBLIC web-fbs)

option(VAST_ENABLE_WEB_UI "Build the web UI" ON)
add_feature_info("VAST_ENABLE_WEB_UI" VAST_ENABLE_WEB_UI "build the web UI.")
if (NOT VAST_ENABLE_WEB_UI)
  return()
endif ()

include(GNUInstallDirs)

# The install dir of the UI root relative to the install prefix of VAST itself.
set(ui_install_dir "${CMAKE_INSTALL_DATADIR}/vast/plugin/web/www")

set(VAST_WEB_UI_BUNDLE
    ""
    CACHE STRING "Specify a path to an external web UI bundle")
if (VAST_WEB_UI_BUNDLE)
  file(REMOVE_RECURSE "${CMAKE_BINARY_DIR}/${ui_install_dir}")
  file(COPY "${VAST_WEB_UI_BUNDLE}/"
       DESTINATION "${CMAKE_BINARY_DIR}/${ui_install_dir}/")
else ()
  # Build and install the frontend as a static site.
  find_program(YARN_PATH yarn)
  if (NOT YARN_PATH)
    message(
      FATAL_ERROR
        "Cannot find 'yarn' in PATH, which is required for building the web frontend"
    )
  endif ()

  # We reconfigure whenever any file in the current directory changes, but exclude
  # files that match the exclude pattern here. Ideally we'd want to use the
  # existing .gitignore file for that, but then we'd need another indirection in
  # CMake, and potential triple escaping is really hard to maintain.
  file(
    GLOB_RECURSE ui_sources
    RELATIVE "${CMAKE_CURRENT_LIST_DIR}"
    CONFIGURE_DEPENDS "${CMAKE_CURRENT_LIST_DIR}/ui/*")
  set(sources_exclude_pattern "^ui/(.svelte-kit|build|node_modules)")
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
      \"${CMAKE_CURRENT_BINARY_DIR}/ui/*\")
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

  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/ui")

  add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/ui.marker"
    COMMENT "Building UI in ${CMAKE_BINARY_DIR}/${ui_install_dir}"
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/ui"
    COMMAND "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_BINARY_DIR}/copy-ui.cmake"
    COMMAND "${YARN_PATH}" install --frozen-lockfile
    COMMAND "${YARN_PATH}" build
    COMMAND "${CMAKE_COMMAND}" -E rm -rf "${CMAKE_BINARY_DIR}/${ui_install_dir}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory
            "${CMAKE_BINARY_DIR}/${ui_install_dir}"
    COMMAND
      "${CMAKE_COMMAND}" -E copy_directory
      "${CMAKE_CURRENT_BINARY_DIR}/ui/build"
      "${CMAKE_BINARY_DIR}/${ui_install_dir}/"
    COMMAND "${CMAKE_COMMAND}" -E touch "${CMAKE_CURRENT_BINARY_DIR}/ui.marker"
    DEPENDS ${ui_sources})

  add_custom_target(web-ui DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/ui.marker")
  add_dependencies(web web-ui)
endif ()

install(
  DIRECTORY "${CMAKE_BINARY_DIR}/${ui_install_dir}/"
  DESTINATION "${ui_install_dir}"
  COMPONENT Runtime)
