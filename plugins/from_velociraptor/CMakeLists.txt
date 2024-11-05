cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  from_velociraptor
  DESCRIPTION "velociraptor plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET from_velociraptor
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

find_package(protobuf CONFIG)
if (NOT protobuf_FOUND)
  find_package(Protobuf MODULE REQUIRED)
  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
  include(protobuf-generate)
  list(POP_BACK CMAKE_MODULE_PATH)
endif ()
find_package(gRPC REQUIRED)

# See this blog post for the gist on how we generate the protobuf and gRPC code:
# https://medium.com/3yourmind/generate-grpc-source-files-using-cmake-ec3acd246b45

add_library(velociraptor-proto STATIC
            "${CMAKE_CURRENT_SOURCE_DIR}/velociraptor.proto")
if (NOT TENZIR_ENABLE_STATIC_EXECUTABLE)
  set_property(TARGET velociraptor-proto PROPERTY POSITION_INDEPENDENT_CODE ON)
endif ()
target_compile_features(velociraptor-proto PRIVATE cxx_std_20)
target_link_libraries(velociraptor-proto PUBLIC protobuf::libprotobuf
                                                gRPC::grpc++)
dependency_summary("protobuf" protobuf::libprotobuf "Dependencies")
dependency_summary("gRPC" gRPC::grpc++ "Dependencies")

set(PROTO_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")

# Generate the Protobuf code.
protobuf_generate(
  TARGET velociraptor-proto
  OUT_VAR PROTO_GENERATED_FILES
  PROTOC_OUT_DIR "${PROTO_BINARY_DIR}")

set_source_files_properties(${PROTO_GENERATED_FILES}
                            PROPERTIES SKIP_UNITY_BUILD_INCLUSION on)

# Generate the gRPC code.
protobuf_generate(
  TARGET velociraptor-proto
  OUT_VAR PROTO_GENERATED_FILES
  LANGUAGE grpc
  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
  PLUGIN "protoc-gen-grpc=\$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
  PROTOC_OUT_DIR "${PROTO_BINARY_DIR}")

set_source_files_properties(${PROTO_GENERATED_FILES}
                            PROPERTIES SKIP_UNITY_BUILD_INCLUSION on)

target_include_directories(from_velociraptor SYSTEM
                           PUBLIC "$<BUILD_INTERFACE:${PROTO_BINARY_DIR}>")

target_link_libraries(from_velociraptor PRIVATE velociraptor-proto)
