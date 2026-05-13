cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  nats
  DESCRIPTION "NATS plugin for Tenzir"
  LANGUAGES CXX)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(cnats CONFIG QUIET)
if (NOT cnats_FOUND)
  message(
    WARNING "Disabling NATS plugin because the nats.c library was not found")
  return()
endif ()
if (TARGET cnats::nats)
  set(nats_library cnats::nats)
elseif (TARGET cnats::nats_static)
  set(nats_library cnats::nats_static)
else ()
  message(
    FATAL_ERROR
      "cnats package did not provide cnats::nats or cnats::nats_static")
endif ()

include(CheckCXXSourceCompiles)
get_target_property(nats_include_directories ${nats_library}
                    INTERFACE_INCLUDE_DIRECTORIES)
set(CMAKE_REQUIRED_INCLUDES "${nats_include_directories}")
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
check_cxx_source_compiles(
  [[
    #include <nats/nats.h>

    int main() {
      auto* fn = &js_PullSubscribeAsync;
      jsOptions opts;
      jsOptions_Init(&opts);
      opts.PullSubscribeAsync.FetchSize = 1;
      opts.PullSubscribeAsync.KeepAhead = 1;
      (void)fn;
      return 0;
    }
  ]]
  TENZIR_NATS_HAS_PULL_SUBSCRIBE_ASYNC)
unset(CMAKE_TRY_COMPILE_TARGET_TYPE)
unset(CMAKE_REQUIRED_INCLUDES)
if (NOT TENZIR_NATS_HAS_PULL_SUBSCRIBE_ASYNC)
  message(WARNING "Disabling NATS plugin because the nats.c library is too old")
  return()
endif ()

find_package(PkgConfig REQUIRED)
pkg_check_modules(Libevent REQUIRED IMPORTED_TARGET libevent libevent_pthreads)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET nats
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp"
  INCLUDE_DIRECTORIES include)

target_link_libraries(nats PUBLIC ${nats_library})
target_include_directories(
  nats BEFORE PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/compat/libevent2")
target_link_libraries(nats PUBLIC PkgConfig::Libevent)
