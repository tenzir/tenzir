cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  gliner
  DESCRIPTION "GLiNER-based entity recognition plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

find_package(onnxruntime QUIET CONFIG)
if (NOT onnxruntime_FOUND)
  message(WARNING "Disabling gliner plugin (ONNX Runtime not found)")
  return()
endif ()

find_path(SENTENCEPIECE_INCLUDE_DIR sentencepiece_processor.h)
find_library(SENTENCEPIECE_LIBRARY sentencepiece)
if (NOT SENTENCEPIECE_INCLUDE_DIR OR NOT SENTENCEPIECE_LIBRARY)
  message(WARNING "Disabling gliner plugin (SentencePiece not found)")
  return()
endif ()

TenzirRegisterPlugin(
  TARGET gliner
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
  TEST_SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp"
  INCLUDE_DIRECTORIES include)

dependency_summary("onnxruntime" onnxruntime::onnxruntime "Dependencies")
dependency_summary("sentencepiece" "${SENTENCEPIECE_LIBRARY}" "Dependencies")
target_include_directories(gliner PRIVATE "${SENTENCEPIECE_INCLUDE_DIR}")
target_link_libraries(gliner PUBLIC onnxruntime::onnxruntime
                                    "${SENTENCEPIECE_LIBRARY}")
