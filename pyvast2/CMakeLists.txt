cmake_minimum_required(VERSION 3.18...3.23 FATAL_ERROR)
project(pyvast LANGUAGES CXX)

find_package(pybind11 REQUIRED)

set(pyvast_sources "${CMAKE_CURRENT_SOURCE_DIR}/src/example.cpp")
pybind11_add_module(pyvast ${pyvast_sources})
target_link_libraries(pyvast PRIVATE vast::internal)
target_link_libraries(pyvast PUBLIC vast::libvast)
set_target_properties(pyvast
  PROPERTIES
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
)

configure_file(__init__.py
  "${CMAKE_CURRENT_BINARY_DIR}/__init__.py"
)
configure_file(setup.py
  "${CMAKE_CURRENT_BINARY_DIR}/setup.py"
)
