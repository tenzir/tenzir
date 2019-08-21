# API documentation
if (DOXYGEN_FOUND)
  set(doxyfile ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
  set(api_site "www.icir.org:vast/api")
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in ${doxyfile} @ONLY)
  add_custom_target(vast-doc
      ${DOXYGEN_EXECUTABLE} ${doxyfile}
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "generating API documentation with Doxygen" VERBATIM)
  add_custom_target(vast-doc-www
      rsync -az -e ssh --delete ${CMAKE_CURRENT_BINARY_DIR}/html ${api_site}
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
      COMMENT "syncing documentation with project site" VERBATIM)
  if (NOT TARGET doc)
    add_custom_target(doc)
  endif ()
  if (NOT TARGET doc-www)
    add_custom_target(doc-www)
  endif ()
  add_dependencies(doc vast-doc)
  add_dependencies(doc-www vast-doc-www)
endif ()

# man pages
if (MD2MAN_FOUND)
  add_custom_target(vast-man)
  add_custom_target(vast-man-replace)
  foreach(man_page vast.1)
    install(FILES ${man_page} DESTINATION share/man/man1)
    if (MD2MAN_FOUND)
      set(man_source ${CMAKE_CURRENT_SOURCE_DIR}/${man_page})
      set(man_generated ${CMAKE_CURRENT_BINARY_DIR}/${man_page})
      month_day_year(MAN_DATE) # bottom-center date in man page
      configure_file(${man_source}.md ${man_generated}.md @ONLY)
      file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/make_man_${man_page}.cmake
        "execute_process(
           COMMAND md2man-roff ${man_generated}.md
           OUTPUT_FILE ${man_generated})")
      add_custom_target(vast-man-make-${man_page}
        ${CMAKE_COMMAND} -P make_man_${man_page}.cmake
        COMMENT "Generating man page ${man_page}")
      add_custom_target(vast-man-replace-${man_page}
        ${CMAKE_COMMAND} -E copy ${man_generated} ${man_source}
        DEPENDS man-make-${man_page}
        COMMENT "Replacing man page ${man_page}")
      add_dependencies(vast-man vast-man-make-${man_page})
      add_dependencies(vast-man-replace vast-man-replace-${man_page})
    endif ()
  endforeach()
  if (NOT TARGET man)
    add_custom_target(man)
  endif ()
  if (NOT TARGET man-replace)
    add_custom_target(man-replace)
  endif ()
  add_dependencies(man vast-man)
  add_dependencies(man-replace vast-man-replace)
endif ()
