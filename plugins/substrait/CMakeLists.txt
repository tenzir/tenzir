cmake_minimum_required(VERSION 3.18...3.22 FATAL_ERROR)

project(
  substrait
  VERSION 0.1.0
  DESCRIPTION "Substrait query language plugin for VAST"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside VAST, it is necessary to specify this
# line manually so plugins can be linked against an installed VAST.
include(CTest)

# FIXME: migrate unit tests
find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET substrait
  ENTRYPOINT  substrait.cpp
  TEST_SOURCES tests.cpp)

# Protobuf support

# Note that the `PROTOBUF_GENERATE_CPP()` function provided by the protobuf
# package only works if the `CMakeLists.txt` is in the same directory as the
# protobuf files. Since substrait has a more complicated protobuf layout,
# and we don't want to patch in a new cmake file, we have to use the more
# complicated way below.

find_package(Protobuf REQUIRED)

file(GLOB_RECURSE substrait_protos CONFIGURE_DEPENDS
  aux/substrait/proto/substrait/**.proto)

set (PROTO_CPP_OUTDIR "${CMAKE_CURRENT_BINARY_DIR}/proto/")
file(MAKE_DIRECTORY ${PROTO_CPP_OUTDIR})

foreach (file ${substrait_protos})
    string(REGEX REPLACE "^.*/aux/substrait/proto/" "" proto_basename ${file})
    # Replace .proto -> .pb.cc
    string(REGEX REPLACE "[.]proto$" ".pb.cc" proto_cc ${proto_basename})
    string(PREPEND proto_cc ${PROTO_CPP_OUTDIR})
    # Replace .proto -> .pb.h
    string(REGEX REPLACE "[.]proto$" ".pb.h" proto_h ${proto_basename})
    string(PREPEND proto_h ${PROTO_CPP_OUTDIR})


    add_custom_command(
            COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} ${PROTO_FLAGS} --cpp_out=${PROTO_CPP_OUTDIR} ${proto_basename}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/aux/substrait/proto
            DEPENDS ${file}
            OUTPUT ${proto_cc} ${proto_h}
    )

    list(APPEND PROTO_HDRS ${proto_h})
    list(APPEND PROTO_SRCS ${proto_cc})
endforeach()

message(info "added source ${PROTO_SRCS}")

target_include_directories(substrait PUBLIC ${PROTO_CPP_OUTDIR})
target_include_directories(substrait PUBLIC include/)
target_sources(substrait PRIVATE ${PROTO_SRCS})
target_link_libraries(substrait PUBLIC protobuf)



