# Miscellaneous CMake helper.

# Joins a list to a string.
function (join values separator output)
  string(REPLACE ";" "${separator}" _tmp_str "${values}")
  set(${output}
      "${_tmp_str}"
      PARENT_SCOPE)
endfunction ()

# Treats target as a system dependency.
function (VASTSystemizeTarget tgt)
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

# Helper utility for printing the status of dependencies.
macro (dependency_summary name what category)
  get_property(VAST_DEPENDENCY_SUMMARY_CATEGORIES GLOBAL
               PROPERTY "VAST_DEPENDENCY_SUMMARY_CATEGORIES_PROPERTY")
  if (NOT "${category}" IN_LIST VAST_DEPENDENCY_SUMMARY_CATEGORIES)
    list(APPEND VAST_DEPENDENCY_SUMMARY_CATEGORIES "${category}")
    set_property(GLOBAL PROPERTY "VAST_DEPENDENCY_SUMMARY_CATEGORIES_PROPERTY"
                                 "${VAST_DEPENDENCY_SUMMARY_CATEGORIES}")
  endif ()
  get_property(VAST_DEPENDENCY_SUMMARY GLOBAL
               PROPERTY "VAST_DEPENDENCY_SUMMARY_${category}_PROPERTY")
  if (TARGET ${what})
    get_target_property(type "${what}" TYPE)
    if (type STREQUAL "INTERFACE_LIBRARY")
      get_target_property(location "${what}" INTERFACE_INCLUDE_DIRECTORIES)
      if (EXISTS "${location}")
        get_filename_component(location "${location}" DIRECTORY)
      endif ()
    else ()
      get_target_property(location "${what}" LOCATION)
      if (EXISTS "${location}")
        get_filename_component(location "${location}" DIRECTORY)
        get_filename_component(location "${location}" DIRECTORY)
      endif ()
    endif ()
  else ()
    set(location "${what}")
  endif ()
  string(GENEX_STRIP "${location}" location)
  if ("${location}" STREQUAL "")
    set(location "Unknown")
  elseif ("${location}" MATCHES "-NOTFOUND$")
    set(location "Not found")
  endif ()
  list(APPEND VAST_DEPENDENCY_SUMMARY " * ${name}: ${location}")
  list(SORT VAST_DEPENDENCY_SUMMARY)
  list(REMOVE_DUPLICATES VAST_DEPENDENCY_SUMMARY)
  set_property(GLOBAL PROPERTY "VAST_DEPENDENCY_SUMMARY_${category}_PROPERTY"
                               "${VAST_DEPENDENCY_SUMMARY}")
endmacro ()
