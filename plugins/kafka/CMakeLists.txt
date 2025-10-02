cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  kafka
  DESCRIPTION "kafka plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(
  AWSSDK
  COMPONENTS core identity-management
  REQUIRED CONFIG)
find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET kafka
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp"
  INCLUDE_DIRECTORIES include)

find_package(RdKafka QUIET)
if (NOT RdKafka_FOUND)
  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
  find_package(RdKafka QUIET REQUIRED)
  list(POP_BACK CMAKE_MODULE_PATH)
endif ()

if (TENZIR_ENABLE_STATIC_EXECUTABLE)
  # rdkafka doesn't resolve transitive dependencies of cyrus_sasl correctly.
  # This is really CMake's fault for not providing a seamlesss integration
  # with pkg-config.
  set_property(
    TARGET RdKafka::rdkafka
    APPEND
    PROPERTY INTERFACE_LINK_LIBRARIES "db")
endif ()

dependency_summary("rdkafka" RdKafka::rdkafka++ "Dependencies")
target_link_libraries(kafka PUBLIC RdKafka::rdkafka++)

target_link_libraries(kafka PUBLIC ${AWSSDK_LINK_LIBRARIES})
