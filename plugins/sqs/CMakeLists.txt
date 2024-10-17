cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  sqs
  DESCRIPTION "SQS plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET sqs
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

find_package(CURL)
find_package(ZLIB)

find_package(AWSSDK COMPONENTS sqs REQUIRED CONFIG)
dependency_summary("AWS SDK" ${AWSSDK_CORE_HEADER_FILE} "Dependencies")

target_link_libraries(sqs PUBLIC ${AWSSDK_LINK_LIBRARIES})
