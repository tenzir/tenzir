cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  kafka
  DESCRIPTION "kafka plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET kafka
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp"
  INCLUDE_DIRECTORIES include)

set(RDKAFKA_BUILD_STATIC
    ON
    CACHE STRING "")
set(RDKAFKA_BUILD_EXAMPLES
    OFF
    CACHE STRING "")
set(RDKAFKA_BUILD_TESTS
    OFF
    CACHE STRING "")

add_subdirectory(aux/librdkafka)
target_sources(
  rdkafka
  PRIVATE aux/librdkafka/src/rdkafka_aws.c
          aux/librdkafka/src/rdkafka_sasl_aws_msk_iam.c
          aux/librdkafka/src/rdstringbuilder.c)
target_compile_definitions(rdkafka PUBLIC WITH_SASL_AWS_MSK_IAM=1)
target_compile_definitions(rdkafka++ PUBLIC WITH_SASL_AWS_MSK_IAM=1)
add_library(RdKafka::rdkafka++ ALIAS rdkafka++)

#find_package(RdKafka QUIET)
#if (NOT RdKafka_FOUND)
#  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
#  find_package(RdKafka QUIET REQUIRED)
#  list(POP_BACK CMAKE_MODULE_PATH)
#endif ()

#dependency_summary("rdkafka" RdKafka::rdkafka++ "Dependencies")
target_link_libraries(kafka PUBLIC RdKafka::rdkafka++)
