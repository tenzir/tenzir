cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  velociraptor
  DESCRIPTION "velociraptor plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

file(GLOB_RECURSE velociraptor_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

TenzirRegisterPlugin(
  TARGET velociraptor
  ENTRYPOINT src/plugin.cpp
  SOURCES ${velociraptor_sources})

find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# See this blog post for the gist on how we generate the protobuf and gRPC code:
# https://medium.com/3yourmind/generate-grpc-source-files-using-cmake-ec3acd246b45

add_library(proto STATIC "${CMAKE_CURRENT_SOURCE_DIR}/velociraptor.proto")
target_link_libraries(proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
dependency_summary("protobuf" protobuf::libprotobuf "Dependencies")
dependency_summary("gRPC" gRPC::grpc++ "Dependencies")

set(PROTO_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")

# Generate the Protobuf code.
protobuf_generate(
    TARGET proto
    OUT_VAR PROTO_GENERATED_FILES
    PROTOC_OUT_DIR "${PROTO_BINARY_DIR}")

set_source_files_properties(${PROTO_GENERATED_FILES} PROPERTIES
                            SKIP_UNITY_BUILD_INCLUSION on)

# Generate the gRPC code.
protobuf_generate(
    TARGET proto
    OUT_VAR PROTO_GENERATED_FILES
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=\$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    PROTOC_OUT_DIR "${PROTO_BINARY_DIR}")

set_source_files_properties(${PROTO_GENERATED_FILES} PROPERTIES
                            SKIP_UNITY_BUILD_INCLUSION on)

target_include_directories(velociraptor SYSTEM PUBLIC
                           "$<BUILD_INTERFACE:${PROTO_BINARY_DIR}>")

target_link_libraries(velociraptor PRIVATE proto)
