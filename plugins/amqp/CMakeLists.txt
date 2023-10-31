cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  amqp
  DESCRIPTION "AMQP plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

file(GLOB_RECURSE rabbitmq_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

TenzirRegisterPlugin(
  TARGET amqp
  ENTRYPOINT src/plugin.cpp
  SOURCES ${rabbitmq_sources}
  INCLUDE_DIRECTORIES include)

find_package(rabbitmq-c QUIET)
if (NOT rabbitmq-c_FOUND)
  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
  find_package(rabbitmq-c QUIET REQUIRED)
  list(POP_BACK CMAKE_MODULE_PATH)
endif ()

target_link_libraries(amqp PUBLIC rabbitmq::rabbitmq)
