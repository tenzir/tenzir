cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  s3
  DESCRIPTION "S3 plugin for Tenzir"
  LANGUAGES CXX)

find_package(Arrow QUIET REQUIRED CONFIG)
if (NOT ARROW_S3)
  message(WARNING "Disabling S3 plugin because the ARROW_S3 flag is not found/enabled. You probably need to enable ARROW_S3 when installing Apache Arrow.")
  return()
endif()

include(CTest)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET s3
  ENTRYPOINT src/plugin.cpp
  INCLUDE_DIRECTORIES include
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp")
