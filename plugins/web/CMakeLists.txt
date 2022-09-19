cmake_minimum_required(VERSION 3.18...3.24 FATAL_ERROR)

project(
  web
  VERSION 1.0.0
  DESCRIPTION "Web plugin for VAST"
  LANGUAGES CXX)

include(CTest)

file(GLOB_RECURSE web_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
     "${CMAKE_CURRENT_SOURCE_DIR}/include/web/*.hpp")

file(GLOB_RECURSE web_tests CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")

# TODO: Remove this warning once we have vendored restinio.
if (APPLE)
  message(WARNING "The web plugin is not supported on macOS")
  return()
endif ()

find_package(VAST REQUIRED)

VASTRegisterPlugin(
  TARGET web
  ENTRYPOINT src/plugin.cpp
  SOURCES ${web_sources}
  TEST_SOURCES ${web_tests}
  INCLUDE_DIRECTORIES include)

# TODO: Vendor restinio and add a workaround to ensure that
# restinio is using our own fmt.
find_package(fmt REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(restinio REQUIRED)

target_link_libraries(web PRIVATE restinio::restinio OpenSSL::SSL
                                  OpenSSL::Crypto)

# Compile the web FlatBuffers schemas.
file(GLOB flatbuffers_schemas CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/fbs/*.fbs")

VASTCompileFlatBuffers(
  TARGET web-fbs
  SCHEMAS ${flatbuffers_schemas}
  INCLUDE_DIRECTORY "web/fbs")

target_link_libraries(web PUBLIC web-fbs)
