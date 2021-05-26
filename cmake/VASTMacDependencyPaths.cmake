include_guard(GLOBAL)

# As of CMake 2.8.3, Fink and MacPorts search paths are appended to the default
# search prefix paths, but the nicer thing would be if they are prepended to the
# default, so that is fixed here.
if (NOT _MAC_DEPENDENCY_PATHS)
  set(_MAC_DEPENDENCY_PATHS)
  if (APPLE AND "${PROJECT_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
    # Macports
    find_program(MAC_PORTS_BIN ports)
    if (MAC_PORTS_BIN)
      list(INSERT CMAKE_PREFIX_PATH 0 /opt/local)
    endif ()
    # Homebrew
    find_program(MAC_HOMEBREW_BIN brew)
    if (MAC_HOMEBREW_BIN)
      execute_process(
        COMMAND ${MAC_HOMEBREW_BIN} "--prefix"
        OUTPUT_VARIABLE HOMEBREW_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE)
      list(INSERT CMAKE_PREFIX_PATH 0 ${HOMEBREW_PREFIX})
      list(INSERT CMAKE_PREFIX_PATH 0 ${HOMEBREW_PREFIX}/opt/openssl)
    endif ()
    # fink
    find_program(MAC_FINK_BIN fink)
    if (MAC_FINK_BIN)
      list(INSERT CMAKE_PREFIX_PATH 0 /sw)
    endif ()
  endif ()
endif ()

mark_as_advanced(MAC_PORTS_BIN MAC_HOMEBREW_BIN MAC_FINK_BIN)
