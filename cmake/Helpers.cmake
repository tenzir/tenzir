# Miscellaneous CMake helper.

# Joins a list to a string.
function (join values separator output)
  string(REPLACE ";" "${separator}" _tmp_str "${values}")
  set(${output}
      "${_tmp_str}"
      PARENT_SCOPE)
endfunction ()

# Retrives a textual representation of the current month.
macro (month result)
  cmake_policy(SET CMP0007 NEW)
  set(months
      ";January;February;March;April;May;June;July;August;September;October;November;December"
  )
  string(TIMESTAMP m "%m")
  list(GET months "${m}" ${result})
endmacro ()

# Retrives the current date in the from "Month Year".
macro (month_year result)
  month(m)
  string(TIMESTAMP y "%Y")
  set(${result} "${m} ${y}")
endmacro ()

# Retrives the current date in the from "Month Day, Year".
macro (month_day_year result)
  month(m)
  string(TIMESTAMP d "%d")
  string(TIMESTAMP y "%Y")
  set(${result} "${m} ${d}, ${y}")
endmacro ()

# Creates a relative symbolic link in the installation directory. Both source
# and destination are interpreted as relative to ${CMAKE_INSTALL_PREFIX}.
macro (install_symlink _filepath _sympath)
  get_filename_component(_symdir ${_sympath} DIRECTORY)
  file(RELATIVE_PATH _rel "${CMAKE_INSTALL_PREFIX}/${_symdir}"
       "${CMAKE_INSTALL_PREFIX}/${_filepath}")
  install(
    CODE "
    set(_destdir \"${CMAKE_INSTALL_PREFIX}\")
    if (NOT \"\$ENV{DESTDIR}\" STREQUAL \"\")
      string(PREPEND _destdir \"\$ENV{DESTDIR}/\")
      set(_destdir )
    endif ()
    set(_dst \"\${_destdir}/${_sympath}\")
    message(STATUS \"Creating:   \${_dst} -> ${_rel}\")
    execute_process(COMMAND \"${CMAKE_COMMAND}\" -E create_symlink
                    \"${_rel}\" \"\${_dst}\")
  ")
endmacro (install_symlink)

# For multi-config generators, the actual binary output path is not
# `${EXECUTABLE_OUTPUT_PATH}`, but rather `${EXECUTABLE_OUTPUT_PATH}/$<CONFIG>`.
# This macro creates a symlink to the actual target file in
# `${EXECUTABLE_OUTPUT_PATH}`, such that the latest compiled build configuration
# can be run using `./${EXECUTABLE_OUTPUT_PATH}/${_target}`.
macro (multi_config_target_symlink _target)
  get_property(_is_multi_config GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
  if (_is_multi_config)
    add_custom_command(
      TARGET "${_target}"
      POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E create_symlink "$<TARGET_FILE:${_target}>"
              "${EXECUTABLE_OUTPUT_PATH}/${_target}"
      COMMENT
        "Linking ${EXECUTABLE_OUTPUT_PATH}/${_target} -> $<TARGET_FILE:${_target}>"
    )
  endif ()
endmacro (multi_config_target_symlink)
