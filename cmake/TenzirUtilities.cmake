# Miscellaneous CMake helper.

# Joins a list to a string.
function (join values separator output)
  string(REPLACE ";" "${separator}" _tmp_str "${values}")
  set(${output}
      "${_tmp_str}"
      PARENT_SCOPE)
endfunction ()

# Treats target as a system dependency.
function (TenzirSystemizeTarget tgt)
  message(STATUS "Treating ${tgt} as a system dependency")
  get_target_property(_includes ${tgt} INTERFACE_INCLUDE_DIRECTORIES)
  set_property(
    TARGET ${tgt}
    APPEND
    PROPERTY INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${_includes}")
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
