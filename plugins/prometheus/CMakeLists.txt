cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  prometheus
  DESCRIPTION "Prometheus Remote Write plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)
find_package(protobuf CONFIG)
if (NOT protobuf_FOUND)
  find_package(Protobuf MODULE REQUIRED)
endif ()

add_library(
  prometheus-proto STATIC
  "${CMAKE_CURRENT_SOURCE_DIR}/proto/prometheus/types.proto"
  "${CMAKE_CURRENT_SOURCE_DIR}/proto/prometheus/remote.proto"
  "${CMAKE_CURRENT_SOURCE_DIR}/proto/io/prometheus/write/v2/types.proto")
if (NOT TENZIR_ENABLE_STATIC_EXECUTABLE)
  set_property(TARGET prometheus-proto PROPERTY POSITION_INDEPENDENT_CODE ON)
endif ()
target_compile_features(prometheus-proto PRIVATE cxx_std_20)
target_link_libraries(prometheus-proto PUBLIC protobuf::libprotobuf)
dependency_summary("protobuf" protobuf::libprotobuf "Dependencies")

set(PROTO_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")
protobuf_generate(
  TARGET prometheus-proto
  OUT_VAR PROTO_GENERATED_FILES
  IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/proto"
  PROTOC_OUT_DIR "${PROTO_BINARY_DIR}")
set_source_files_properties(${PROTO_GENERATED_FILES}
                            PROPERTIES SKIP_UNITY_BUILD_INCLUSION on)
target_include_directories(prometheus-proto
                           PUBLIC "$<BUILD_INTERFACE:${PROTO_BINARY_DIR}>")

TenzirRegisterPlugin(
  TARGET prometheus
  ENTRYPOINT src/plugin.cpp
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp")

target_link_libraries(prometheus PRIVATE prometheus-proto)
