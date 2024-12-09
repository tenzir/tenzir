cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  google-cloud-pubsub
   DESCRIPTION "Google Cloud Pub/Sub plugin for Tenzir"
  LANGUAGES CXX)

find_package(google_cloud_cpp_pubsub CONFIG)
if (NOT google_cloud_cpp_pubsub_FOUND)
  message(WARNING "Disabling Google Cloud Pub/Sub plugin because the google-could-cpp library was not found")
  return()
endif()

include(CTest)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET google-cloud-pubsub
  ENTRYPOINT src/plugin.cpp
  INCLUDE_DIRECTORIES include
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp")

target_link_libraries(google-cloud-pubsub PUBLIC google-cloud-cpp::pubsub)
