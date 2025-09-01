# Get all propreties that cmake supports
if (NOT CMAKE_PROPERTY_LIST)
  execute_process(COMMAND ${CMAKE_COMMAND} --help-property-list
                  OUTPUT_VARIABLE CMAKE_PROPERTY_LIST)

  # Convert command output into a CMake list
  string(REGEX REPLACE ";" "\\\\;" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
  string(REGEX REPLACE "\n" ";" CMAKE_PROPERTY_LIST "${CMAKE_PROPERTY_LIST}")
  list(REMOVE_DUPLICATES CMAKE_PROPERTY_LIST)
endif ()

function (print_properties)
  message("CMAKE_PROPERTY_LIST = ${CMAKE_PROPERTY_LIST}")
endfunction ()

function (print_target_properties target)
  if (NOT TARGET ${target})
    message(STATUS "There is no target named '${target}'")
    return()
  endif ()

  foreach (property ${CMAKE_PROPERTY_LIST})
    string(REPLACE "<CONFIG>" "${CMAKE_BUILD_TYPE}" property ${property})

    # Fix https://stackoverflow.com/questions/32197663/how-can-i-remove-the-the-location-property-may-not-be-read-from-target-error-i
    if (property STREQUAL "LOCATION"
        OR property MATCHES "^LOCATION_"
        OR property MATCHES "_LOCATION$")
      continue()
    endif ()

    get_property(
      was_set
      TARGET ${target}
      PROPERTY ${property}
      SET)
    if (was_set)
      get_target_property(value ${target} ${property})
      message("${target} ${property} = ${value}")
    endif ()
  endforeach ()
endfunction ()

set(visited "")

function (print_target_properties_recursive target)
  print_target_properties(${target})
  get_target_property(ideps ${target} INTERFACE_LINK_LIBRARIES)
  get_target_property(ldeps ${target} INTERFACE_LINK_LIBRARIES)
  set(deps ${ideps} ${ldeps})
  list(REMOVE_DUPLICATES deps)
  if ("${deps}" STREQUAL "deps-NOTFOUND")
    return()
  endif ()
  foreach (dep ${deps})
    if (${dep} IN_LIST visited)
      continue()
    endif ()
    list(APPEND visited ${dep})
    set(visited
        ${visited}
        PARENT_SCOPE)
    print_target_properties_recursive(${dep})
  endforeach ()
endfunction ()
