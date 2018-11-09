## Configure data.hpp

# Bro logs.
set(test_data "#ifndef TEST_DATA\n#define TEST_DATA")
set(test_data "${test_data}\n\nnamespace bro {\n")
file(GLOB logs RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" artefacts/logs/bro/*)
foreach (log ${logs})
  get_filename_component(log_basename ${log} NAME_WE)
  set (this "constexpr auto ${log_basename} = VAST_TEST_PATH\"${log}\";")
  set (test_data "${test_data}\n${this}")
endforeach ()
set(test_data "${test_data}\n\n} // namespace bro")

# Bgpdump logs.
set(test_data "${test_data}\n\nnamespace bgpdump {\n")
file(GLOB logs RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" artefacts/logs/bgpdump/*.txt)
foreach (log ${logs})
  get_filename_component(log_basename ${log} NAME_WE)
  set (this "constexpr auto ${log_basename} = VAST_TEST_PATH\"${log}\";")
  set (test_data "${test_data}\n${this}")
endforeach ()
set(test_data "${test_data}\n\n} // namespace bgpdump")

# MRT logs.
set(test_data "${test_data}\n\nnamespace mrt {\n")
file(GLOB logs RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" artefacts/logs/mrt/*)
foreach (log ${logs})
  get_filename_component(log_basename ${log} NAME_WE)
  set (this "constexpr auto ${log_basename} = VAST_TEST_PATH\"${log}\";")
  set (test_data "${test_data}\n${this}")
endforeach ()
set(test_data "${test_data}\n\n} // namespace mrt")

# PCAP traces.
set(test_data "${test_data}\n\nnamespace traces {\n")
file(GLOB traces RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" artefacts/traces/*.pcap)
foreach (trace ${traces})
  get_filename_component(trace_basename ${trace} NAME_WE)
  set(this "constexpr auto ${trace_basename} = VAST_TEST_PATH\"${trace}\";")
  set(test_data "${test_data}\n${this}")
endforeach ()
set(test_data "${test_data}\n\n} // namespace traces")
set(test_data "${test_data}\n\n#endif")

configure_file(vast/test/data.hpp.in ${CMAKE_CURRENT_BINARY_DIR}/vast/test/data.hpp)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/vast/test/data.hpp"
        DESTINATION include/vast/test
)

## test library definition

add_library(libvast_test SHARED
  src/actor_system.cpp
  src/events.cpp
  src/node.cpp
  src/symbols.cpp
)
target_compile_definitions(libvast_test PUBLIC
  $<BUILD_INTERFACE:VAST_TEST_PATH=\"${CMAKE_CURRENT_SOURCE_DIR}/\">
  $<INSTALL_INTERFACE:VAST_TEST_PATH=\"${CMAKE_INSTALL_DATADIR}/vast/test/\">
)
target_include_directories(libvast_test PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_include_directories(libvast_test PRIVATE
  ${CMAKE_CURRENT_BINARY_DIR}/vast/test
  ${CMAKE_CURRENT_SOURCE_DIR}/vast/test
)
target_link_libraries(libvast_test PUBLIC libvast)
set(isExe $<STREQUAL:$<TARGET_PROPERTY:TYPE>,EXECUTABLE>)
target_sources(libvast_test INTERFACE
    "$<${isExe}:$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp>>"
    "$<${isExe}:$<INSTALL_INTERFACE:$<INSTALL_PREFIX>/${CMAKE_INSTALL_DATADIR}/vast/test/main.cpp>>"
)
set_target_properties(libvast_test
  PROPERTIES
  SOVERSION "${VERSION_MAJOR}"
  VERSION "${VERSION_MAJOR}.${VERSION_MINOR}"
  OUTPUT_NAME vast_test
  EXPORT_NAME test
)
install(TARGETS libvast_test
  EXPORT VASTTargets
  DESTINATION lib
)
install(FILES src/main.cpp DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/test")
install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/vast"
  DESTINATION include
)
install(
  DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/artefacts"
  DESTINATION "${CMAKE_INSTALL_DATADIR}/vast/test"
)
