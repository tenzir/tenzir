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

# man pages
if (PANDOC_FOUND)
  function (add_vast_manpage _target)
    set(man_page vast.1)
    set(man_generated "${CMAKE_CURRENT_BINARY_DIR}/${man_page}")
    file(
      WRITE ${CMAKE_CURRENT_BINARY_DIR}/make_man_${man_page}.cmake
      "set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
      include(Helpers)
      execute_process(
        COMMAND ${CMAKE_BINARY_DIR}/bin/vast manual
        ERROR_FILE ${man_generated}.md.in)
      month_day_year(MAN_DATE)
      configure_file(${man_generated}.md.in ${man_generated}.md.tmp @ONLY)
      set(changed TRUE)
      if (EXISTS ${man_generated}.md)
        file(SHA3_224 ${man_generated}.md incumbent)
        file(SHA3_224 ${man_generated}.md.tmp challenger)
        if (incumbent STREQUAL challenger)
          set(changed FALSE)
        endif ()
      endif ()
      if (changed)
        file(RENAME ${man_generated}.md.tmp ${man_generated}.md)
        execute_process(
           COMMAND pandoc -s -f markdown -t man ${man_generated}.md
           OUTPUT_FILE ${man_generated})
      endif()")
    install(
      FILES "${man_generated}"
      DESTINATION "${CMAKE_INSTALL_MANDIR}/man1"
      OPTIONAL)
    add_custom_command(
      TARGET ${_target}
      POST_BUILD
      BYPRODUCTS ${man_generated}
      COMMAND ${CMAKE_COMMAND} -P make_man_${man_page}.cmake
      COMMENT "Generating man page ${man_page}")
  endfunction ()
else ()
  function (add_vast_manpage _target)

  endfunction ()
endif ()
