include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_subdirectory(lz4)

# This variable comprises all source which we directly compile into libvast.
set(aux_sources
  ${CMAKE_CURRENT_SOURCE_DIR}/xxhash/xxhash.c
  PARENT_SCOPE
)
