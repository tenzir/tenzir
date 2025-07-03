cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  ai
  DESCRIPTION "AI plugin for Tenzir"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside Tenzir, it is necessary to specify this
# line manually so plugins can be linked against an installed Tenzir.
include(CTest)

option(TENZIR_ENABLE_BUNDLED_AI_SDK_CPP "Use the ai-sdk-cpp submodule" ON)
add_feature_info("TENZIR_ENABLE_BUNDLED_AI_SDK_CPP" TENZIR_ENABLE_BUNDLED_AI_SDK_CPP
                   "use the ai-sdk-cpp submodule.")
if (NOT TENZIR_ENABLE_BUNDLED_AI_SDK_CPP)
  find_package(ai-sdk-cpp CONFIG)
endif ()
if (NOT ai-sdk-cpp_FOUND)
  if (NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/aux/ai-sdk-cpp/CMakeLists.txt")
    message(FATAL_ERROR "ai-sdk-cpp library not found, either use -Dai-sdk-cpp_DIR=... or"
                        " initialize the libtenzir/aux/ai-sdk-cpp submodule")
  endif ()
  add_subdirectory(aux/ai-sdk-cpp)
  dependency_summary("ai-sdk-cpp" "${CMAKE_CURRENT_SOURCE_DIR}/aux/ai-sdk-cpp"
                     "Dependencies")
else ()
  dependency_summary("ai-sdk-cpp" ai-sdk-cpp "Dependencies")
endif ()

find_package(Tenzir REQUIRED)
TenzirRegisterPlugin(
  TARGET ai
  ENTRYPOINT src/plugin.cpp
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp")

dependency_summary("ai-sdk-cpp" ai::sdk "Dependencies")
target_link_libraries(ai PUBLIC ai::sdk)
