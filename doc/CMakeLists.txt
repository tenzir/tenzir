find_package(Doxygen QUIET)

# API documentation
if (DOXYGEN_FOUND)
  set(doxyfile ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
  set(api_site "www.icir.org:vast/api")
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${doxyfile} @ONLY)
  add_custom_target(
    vast-doc
    ${DOXYGEN_EXECUTABLE} ${doxyfile}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "generating API documentation with Doxygen"
    VERBATIM)
  add_custom_target(
    vast-doc-www
    rsync
    -az
    -e
    ssh
    --delete
    ${CMAKE_CURRENT_BINARY_DIR}/html
    ${api_site}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "syncing documentation with project site"
    VERBATIM)
  if (NOT TARGET doc)
    add_custom_target(doc)
  endif ()
  if (NOT TARGET doc-www)
    add_custom_target(doc-www)
  endif ()
  add_dependencies(doc vast-doc)
  add_dependencies(doc-www vast-doc-www)
endif ()
