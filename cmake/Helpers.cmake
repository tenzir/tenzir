# Miscellaneous CMake helper.

# Joins a list to a string.
function(join values separator output)
    string (REPLACE ";" "${separator}" _tmp_str "${values}")
    set (${output} "${_tmp_str}" PARENT_SCOPE)
endfunction()

# Retrives a textual representation of the current month.
macro(month result)
  set(months ";January;February;March;April;May;June;July;August;September;October;November;December")
  string(TIMESTAMP m "%m")
  list(GET months "${m}" ${result})
endmacro()

# Retrives the current date in the from "Month Year".
macro(month_year result)
  month(m)
  string(TIMESTAMP y "%Y")
  set(${result} "${m} ${y}")
endmacro()

# Retrives the current date in the from "Month Day, Year".
macro(month_day_year result)
  month(m)
  string(TIMESTAMP d "%d")
  string(TIMESTAMP y "%Y")
  set(${result} "${m} ${d}, ${y}")
endmacro()
