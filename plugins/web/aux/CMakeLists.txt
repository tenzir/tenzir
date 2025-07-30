find_program(PATCH_EXECUTABLE patch REQUIRED)

if (NOT EXISTS "${CMAKE_CURRENT_LIST_DIR}/restinio/.git")
  message(
    FATAL_ERROR
      "submodule '${CMAKE_CURRENT_LIST_DIR}/restinio' is not initialized")
endif ()

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
  DEPENDS "${CMAKE_CURRENT_LIST_DIR}/expected.patch"
          ${restinio_sources_original}
  WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/patched-restinio"
  # Copy the patched restinio library to the binary directory.
  COMMAND
    "${CMAKE_COMMAND}" -E copy_directory
    "${CMAKE_CURRENT_LIST_DIR}/restinio/dev/"
    "${CMAKE_CURRENT_BINARY_DIR}/patched-restinio"
  # Apply a patch to the restinio library that makes some changes.
  COMMAND "${PATCH_EXECUTABLE}" -p2 -i
          "${CMAKE_CURRENT_LIST_DIR}/expected.patch")

add_custom_target(patch-restinio ALL DEPENDS ${restinio_sources_patched})

add_library(restinio INTERFACE)
add_dependencies(restinio patch-restinio)
target_compile_definitions(restinio INTERFACE RESTINIO_USE_BOOST_ASIO)

find_package(Boost REQUIRED COMPONENTS headers)
find_package(OpenSSL REQUIRED)
find_package(fmt REQUIRED)

# Set CMAKE_CONFIGURATION_TYPES to bypass llhttp's build type validation
set(CMAKE_CONFIGURATION_TYPES_BACKUP "${CMAKE_CONFIGURATION_TYPES}")
set(CMAKE_CONFIGURATION_TYPES "${CMAKE_BUILD_TYPE}" CACHE STRING "" FORCE)
# Force llhttp to build as static library to avoid runtime dependency issues
set(BUILD_SHARED_LIBS_BACKUP "${BUILD_SHARED_LIBS}")
set(BUILD_STATIC_LIBS_BACKUP "${BUILD_STATIC_LIBS}")
set(BUILD_SHARED_LIBS OFF)
set(BUILD_STATIC_LIBS ON)
# Enable position-independent code for shared library compatibility
set(CMAKE_POSITION_INDEPENDENT_CODE_BACKUP "${CMAKE_POSITION_INDEPENDENT_CODE}")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_subdirectory(llhttp)
# Restore everything afterwards
set(CMAKE_POSITION_INDEPENDENT_CODE "${CMAKE_POSITION_INDEPENDENT_CODE_BACKUP}")
set(BUILD_SHARED_LIBS "${BUILD_SHARED_LIBS_BACKUP}")
set(BUILD_STATIC_LIBS "${BUILD_STATIC_LIBS_BACKUP}")
set(CMAKE_CONFIGURATION_TYPES "${CMAKE_CONFIGURATION_TYPES_BACKUP}" CACHE STRING "" FORCE)

# Create an interface library for the modified restinio sources.
target_include_directories(
  restinio INTERFACE "${CMAKE_CURRENT_BINARY_DIR}/patched-restinio")
target_link_libraries(
  restinio INTERFACE fmt::fmt Boost::headers OpenSSL::SSL OpenSSL::Crypto
                     llhttp::llhttp)

add_library(restinio::restinio ALIAS restinio)
