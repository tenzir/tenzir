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

option(TENZIR_ENABLE_BUNDLED_LIBRDKAFKA "Use the librdkafka submodule" ON)
add_feature_info("TENZIR_ENABLE_BUNDLED_LIBRDKAFKA" TENZIR_ENABLE_BUNDLED_LIBRDKAFKA
                 "use the librdkafka submodule.")

if (TENZIR_ENABLE_BUNDLED_LIBRDKAFKA)
  set(RDKAFKA_BUILD_STATIC
      ON
      CACHE STRING "")
  set(RDKAFKA_BUILD_EXAMPLES
      OFF
      CACHE STRING "")
  set(RDKAFKA_BUILD_TESTS
      OFF
      CACHE STRING "")

  find_package(LibXml2 REQUIRED)
  find_package(PkgConfig REQUIRED)

  pkg_check_modules(PC_SASL2 IMPORTED_TARGET libsasl2)

  add_subdirectory(aux/librdkafka)
  # The fork we're using does not adjust the CMake build scaffold, so we fix it
  # up here.
  target_sources(
    rdkafka
    PRIVATE aux/librdkafka/src/rdkafka_aws.c
            aux/librdkafka/src/rdkafka_sasl_aws_msk_iam.c
            aux/librdkafka/src/rdstringbuilder.c)
  target_compile_definitions(rdkafka PUBLIC WITH_SASL_AWS_MSK_IAM=1)
  target_compile_definitions(rdkafka++ PUBLIC WITH_SASL_AWS_MSK_IAM=1)
  target_link_libraries(rdkafka PRIVATE LibXml2::LibXml2 PkgConfig::PC_SASL2)

  message(WARNING "add lib")
  add_library(RdKafka::rdkafka++ ALIAS rdkafka++)

  target_compile_definitions(kafka PUBLIC TENZIR_ENABLE_BUNDLED_LIBRDKAFKA=1)
else ()
  find_package(RdKafka QUIET)
  if (NOT RdKafka_FOUND)
    list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
    find_package(RdKafka QUIET REQUIRED)
    list(POP_BACK CMAKE_MODULE_PATH)
  endif ()

  target_compile_definitions(kafka PUBLIC TENZIR_ENABLE_BUNDLED_LIBRDKAFKA=0)
  dependency_summary("rdkafka" RdKafka::rdkafka++ "Dependencies")
endif ()

target_link_libraries(kafka PUBLIC RdKafka::rdkafka++)
