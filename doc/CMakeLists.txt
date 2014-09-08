set(doxyfile ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
set(api_site "www.icir.org:vast/api")

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${doxyfile} @ONLY)

add_custom_target(doc
    ${DOXYGEN_EXECUTABLE} ${doxyfile}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Generating API documentation with Doxygen" VERBATIM)

add_custom_target(doc-www
    rsync -az -e ssh --delete ${CMAKE_CURRENT_BINARY_DIR}/html ${api_site}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Syncing documentation with project site" VERBATIM)
