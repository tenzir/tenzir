# -- Project Setup ------------------------------------------------------------
project(broker C CXX)

# -- libroker -----------------------------------------------------------------

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/broker/VERSION" BROKER_VERSION
     LIMIT_COUNT 1)
string(REPLACE "." " " _version_numbers ${BROKER_VERSION})
separate_arguments(_version_numbers)
list(GET _version_numbers 0 BROKER_VERSION_MAJOR)
list(GET _version_numbers 1 BROKER_VERSION_MINOR)

# The SO number shall increase only if binary interface changes.
set(BROKER_SOVERSION 2)

install(
  DIRECTORY broker/broker
  DESTINATION include
  FILES_MATCHING
  PATTERN "*.hh")

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/broker/src/config.hh.in
               ${CMAKE_CURRENT_BINARY_DIR}/include/broker/config.hh)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/include/broker/config.hh
        DESTINATION include/broker)

set(BROKER_SRC
    broker/3rdparty/sqlite3.c
    broker/src/address.cc
    broker/src/configuration.cc
    broker/src/core_actor.cc
    broker/src/data.cc
    broker/src/defaults.cc
    broker/src/detail/abstract_backend.cc
    broker/src/detail/clone_actor.cc
    broker/src/detail/core_policy.cc
    broker/src/detail/data_generator.cc
    broker/src/detail/filesystem.cc
    broker/src/detail/flare.cc
    broker/src/detail/flare_actor.cc
    broker/src/detail/generator_file_reader.cc
    broker/src/detail/generator_file_writer.cc
    broker/src/detail/make_backend.cc
    broker/src/detail/master_actor.cc
    broker/src/detail/master_resolver.cc
    broker/src/detail/memory_backend.cc
    broker/src/detail/meta_command_writer.cc
    broker/src/detail/meta_data_writer.cc
    broker/src/detail/network_cache.cc
    broker/src/detail/prefix_matcher.cc
    broker/src/detail/sqlite_backend.cc
    broker/src/endpoint.cc
    broker/src/error.cc
    broker/src/internal_command.cc
    broker/src/mailbox.cc
    broker/src/network_info.cc
    broker/src/peer_status.cc
    broker/src/port.cc
    broker/src/publisher.cc
    broker/src/status.cc
    broker/src/status_subscriber.cc
    broker/src/store.cc
    broker/src/subnet.cc
    broker/src/subscriber.cc
    broker/src/time.cc
    broker/src/topic.cc
    broker/src/version.cc)

if (NOT INSTALL_LIB_DIR)
  set(INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif ()

add_library(broker ${BROKER_SRC})
target_include_directories(
  broker SYSTEM
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/broker/include
         ${CMAKE_CURRENT_BINARY_DIR}/include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/broker/3rdparty)
set_target_properties(
  broker
  PROPERTIES CXX_STANDARD 17
             SOVERSION ${BROKER_SOVERSION}
             VERSION ${BROKER_VERSION_MAJOR}.${BROKER_VERSION_MINOR}
             MACOSX_RPATH true
             OUTPUT_NAME broker)
target_compile_definitions(broker PRIVATE SQLITE_OMIT_LOAD_EXTENSION)
# For Clang 10, we need to additionally silence -Wimplicit-int-float-conversion,
# since it is not included in the ignored warnings list for -isystem yet.
target_compile_options(
  broker
  PRIVATE -Wno-implicit-int-float-conversion -Wno-maybe-uninitialized
          -Wno-missing-field-initializers -Wno-unknown-warning-option
          -Wno-unused -Wno-unused-parameter)
target_link_libraries(broker PUBLIC CAF::core CAF::io CAF::openssl)
install(TARGETS broker DESTINATION ${INSTALL_LIB_DIR})

add_library(zeek::broker ALIAS broker)
