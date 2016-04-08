configure_file(${CMAKE_CURRENT_SOURCE_DIR}/vast/config.hpp.in
               ${CMAKE_CURRENT_BINARY_DIR}/vast/config.hpp)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(libvast_sources
  ${aux_sources}
  src/address.cpp
  src/announce.cpp
  src/banner.cpp
  src/bitvector.cpp
  src/bitstream.cpp
  src/bitstream_polymorphic.cpp
  src/block.cpp
  src/chunk.cpp
  src/cleanup.cpp
  src/data.cpp
  src/die.cpp
  src/expression.cpp
  src/event.cpp
  src/filesystem.cpp
  src/http.cpp
  src/individual.cpp
  src/logger.cpp
  src/operator.cpp
  src/pattern.cpp
  src/port.cpp
  src/schema.cpp
  src/subnet.cpp
  src/time.cpp
  src/type.cpp
  src/uuid.cpp
  src/value.cpp
  src/actor/accountant.cpp
  src/actor/archive.cpp
  src/actor/exporter.cpp
  src/actor/importer.cpp
  src/actor/identifier.cpp
  src/actor/index.cpp
  src/actor/key_value_store.cpp
  src/actor/node.cpp
  src/actor/partition.cpp
  src/actor/signal_monitor.cpp
  src/actor/task.cpp
  src/actor/sink/ascii.cpp
  src/actor/sink/bro.cpp
  src/actor/sink/csv.cpp
  src/actor/sink/json.cpp
  src/actor/sink/spawn.cpp
  src/actor/source/bro.cpp
  src/actor/source/bgpdump.cpp
  src/actor/source/spawn.cpp
  src/actor/source/test.cpp
  src/concept/convertible/vast/address.cpp
  src/concept/convertible/vast/data.cpp
  src/concept/convertible/vast/event.cpp
  src/concept/convertible/vast/pattern.cpp
  src/concept/convertible/vast/port.cpp
  src/concept/convertible/vast/schema.cpp
  src/concept/convertible/vast/subnet.cpp
  src/concept/convertible/vast/time.cpp
  src/concept/convertible/vast/type.cpp
  src/concept/convertible/vast/value.cpp
  src/concept/serializable/hierarchy.cpp
  src/detail/adjust_resource_consumption.cpp
  src/expr/evaluator.cpp
  src/expr/normalize.cpp
  src/expr/predicatizer.cpp
  src/expr/restrictor.cpp
  src/expr/resolver.cpp
  src/expr/validator.cpp
  src/io/actor_stream.cpp
  src/io/algorithm.cpp
  src/io/array_stream.cpp
  src/io/buffered_stream.cpp
  src/io/coded_stream.cpp
  src/io/compressed_stream.cpp
  src/io/device.cpp
  src/io/file_stream.cpp
  src/io/getline.cpp
  src/io/iterator.cpp
  src/io/stream_device.cpp
  src/io/stream.cpp
  src/util/fdistream.cpp
  src/util/fdinbuf.cpp
  src/util/fdostream.cpp
  src/util/fdoutbuf.cpp
  src/util/posix.cpp
  src/util/string.cpp
  src/util/system.cpp
  src/util/terminal.cpp
  src/util/detail/posix.cpp
  src/util/hash/crc.cpp
  src/util/hash/murmur.cpp
  src/util/hash/xxhash.cpp
)

if (GPERFTOOLS_FOUND)
  set(libvast_sources ${libvast_sources}
    src/actor/profiler.cpp)
endif ()

if (PCAP_FOUND)
  set(libvast_sources ${libvast_sources}
    src/actor/sink/pcap.cpp
    src/actor/source/pcap.cpp)
endif ()

set(libvast_libs ${CAF_LIBRARIES})

if (VAST_ENABLE_ASSERTIONS)
  set(libvast_libs ${libvast_libs} ${Backtrace_LIBRARIES})
endif ()

if (PCAP_FOUND)
  set(libvast_libs ${libvast_libs} ${PCAP_LIBRARIES})
endif ()

if (BROCCOLI_FOUND)
  set(libvast_libs ${libvast_libs} ${BROCCOLI_LIBRARIES})
endif ()

# Always link with -lprofile if we have Gperftools.
if (GPERFTOOLS_FOUND)
  set(libvast_libs ${libvast_libs} ${GPERFTOOLS_PROFILER})
endif ()

# Only link against tcmalloc if requested.
if (GPERFTOOLS_FOUND AND VAST_USE_PERFTOOLS_HEAP_PROFILER)
  set(libvast_libs ${libvast_libs} ${GPERFTOOLS_TCMALLOC})
endif ()

add_library(libvast SHARED ${libvast_sources})
set_target_properties(libvast
  PROPERTIES
  SOVERSION ${VERSION_MAJOR}
  VERSION ${VERSION_MAJOR}.${VERSION_MINOR}
  OUTPUT_NAME vast)

target_link_libraries(libvast ${libvast_libs})

install(TARGETS libvast DESTINATION lib)
install(DIRECTORY vast/
        DESTINATION include/vast FILES_MATCHING PATTERN "*.hpp")

add_subdirectory(test)
