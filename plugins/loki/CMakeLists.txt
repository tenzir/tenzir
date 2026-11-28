cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  loki
  DESCRIPTION "Grafana Loki integration plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)
find_package(protobuf CONFIG)
if (NOT protobuf_FOUND)
  find_package(Protobuf MODULE REQUIRED)
endif ()

add_library(loki-proto STATIC
            "${CMAKE_CURRENT_SOURCE_DIR}/proto/loki/push.proto")
if (NOT TENZIR_ENABLE_STATIC_EXECUTABLE)
  set_property(TARGET loki-proto PROPERTY POSITION_INDEPENDENT_CODE ON)
endif ()
target_compile_features(loki-proto PRIVATE cxx_std_20)
target_link_libraries(loki-proto PUBLIC protobuf::libprotobuf)
dependency_summary("protobuf" protobuf::libprotobuf "Dependencies")

set(PROTO_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")
protobuf_generate(
  TARGET loki-proto
  OUT_VAR PROTO_GENERATED_FILES
  IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/proto"
  PROTOC_OUT_DIR "${PROTO_BINARY_DIR}")
set_source_files_properties(${PROTO_GENERATED_FILES}
                            PROPERTIES SKIP_UNITY_BUILD_INCLUSION on)
target_include_directories(loki-proto
                           PUBLIC "$<BUILD_INTERFACE:${PROTO_BINARY_DIR}>")

TenzirRegisterPlugin(
  TARGET loki
  ENTRYPOINT src/plugin.cpp
  SOURCES src/push.cpp
  INCLUDE_DIRECTORIES include
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp"
  TEST_SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")

target_link_libraries(loki PRIVATE loki-proto)
if (TARGET loki-test)
  target_link_libraries(loki-test PRIVATE loki-proto)
endif ()
