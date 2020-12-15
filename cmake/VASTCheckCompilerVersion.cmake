# Compiler requirements
set(CLANG_MINIMUM_VERSION 8.0)
set(APPLE_CLANG_MINIMUM_VERSION 9.1)
set(GCC_MINIMUM_VERSION 8.0)

# Ensures that the specified C++ compiler meets our version requirements.
macro (check_compiler_version version)
  message(STATUS "Using ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${version})
    message(FATAL_ERROR "${CMAKE_CXX_COMPILER_VERSION} >= ${version} required")
  endif ()
endmacro ()
