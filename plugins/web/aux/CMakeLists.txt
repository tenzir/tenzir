find_program(PATCH_EXECUTABLE patch REQUIRED)

if (NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/patched-restinio")
  file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/patched-restinio")
endif ()

file(
  GLOB_RECURSE restinio_sources_relative
  RELATIVE "${CMAKE_CURRENT_LIST_DIR}/restinio/dev/"
  CONFIGURE_DEPENDS "${CMAKE_CURRENT_LIST_DIR}/restinio/dev/restinio/*.hpp")

set(restinio_sources_original ${restinio_sources_relative})
list(TRANSFORM restinio_sources_original
     PREPEND "${CMAKE_CURRENT_LIST_DIR}/restinio/dev/")

set(restinio_sources_patched ${restinio_sources_relative})
list(TRANSFORM restinio_sources_patched
     PREPEND "${CMAKE_CURRENT_BINARY_DIR}/patched-restinio/")

add_custom_command(
  OUTPUT ${restinio_sources_patched}
  DEPENDS "${CMAKE_CURRENT_LIST_DIR}/restinio.patch"
          ${restinio_sources_original}
  WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/patched-restinio"
  # Copy the patched restinio library to the binary directory.
  COMMAND
    "${CMAKE_COMMAND}" -E copy_directory
    "${CMAKE_CURRENT_LIST_DIR}/restinio/dev/"
    "${CMAKE_CURRENT_BINARY_DIR}/patched-restinio"
  # Apply a patch to the restinio library that makes some changes.
  COMMAND "${PATCH_EXECUTABLE}" -p1 -i
          "${CMAKE_CURRENT_LIST_DIR}/restinio.patch")

add_custom_target(patch-restinio ALL DEPENDS ${restinio_sources_patched})

add_library(restinio INTERFACE)
add_dependencies(restinio patch-restinio)

find_package(Asio REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(fmt REQUIRED)
find_package(http_parser REQUIRED)

# Create an interface library for the modified restinio sources.
target_include_directories(
  restinio INTERFACE "${CMAKE_CURRENT_BINARY_DIR}/patched-restinio")
target_link_libraries(
  restinio INTERFACE fmt::fmt Asio::Asio OpenSSL::SSL OpenSSL::Crypto
                     http_parser::http_parser)
