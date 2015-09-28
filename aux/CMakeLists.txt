include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# This variable comprises all source which we directly compile into libvast.
set(aux_sources
  ${CMAKE_CURRENT_SOURCE_DIR}/lz4/lz4.c
  ${CMAKE_CURRENT_SOURCE_DIR}/lz4/lz4hc.c
  ${CMAKE_CURRENT_SOURCE_DIR}/xxhash/xxhash.c
  PARENT_SCOPE
)
