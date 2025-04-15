cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  sqs
  DESCRIPTION "SQS plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(AWSSDK COMPONENTS sqs REQUIRED CONFIG)
find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET sqs
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
  INCLUDE_DIRECTORIES include
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp")

find_package(CURL)
find_package(ZLIB)

dependency_summary("AWS SDK" ${AWSSDK_CORE_HEADER_FILE} "Dependencies")

message(STATUS "AWSSDK_LINK_LIBRARIES: ${AWSSDK_LINK_LIBRARIES}")

target_link_libraries(sqs PUBLIC ${AWSSDK_LINK_LIBRARIES})
