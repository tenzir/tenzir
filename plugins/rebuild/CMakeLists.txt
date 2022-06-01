cmake_minimum_required(VERSION 3.18...3.23 FATAL_ERROR)

project(
  rebuild
  VERSION 1.0.0
  DESCRIPTION "Rebuild command plugin for VAST"
  LANGUAGES CXX)

include(CTest)

find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET rebuild
  ENTRYPOINT rebuild.cpp)
