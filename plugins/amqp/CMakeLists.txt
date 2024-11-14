cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  amqp
  DESCRIPTION "AMQP plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET amqp
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp"
  INCLUDE_DIRECTORIES include)

find_package(rabbitmq-c REQUIRED)

if (TENZIR_ENABLE_STATIC_PLUGINS)
  set(rabbitmq_target rabbitmq::rabbitmq-static)
else ()
  set(rabbitmq_target rabbitmq::rabbitmq)
endif ()

dependency_summary("rabbitmq-c" ${rabbitmq_target} "Dependencies")
target_link_libraries(amqp PUBLIC ${rabbitmq_target})
