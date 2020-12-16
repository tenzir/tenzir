# Ensures that the specified C++ compiler meets our version requirements.
macro (check_compiler_version version)
  message(STATUS "Using ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${version})
    message(FATAL_ERROR "${CMAKE_CXX_COMPILER_VERSION} >= ${version} required")
  endif ()
endmacro ()
