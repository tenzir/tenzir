# Compatibility shim for CMake 4.x which removed FindBoost.cmake
# This redirects to Boost's native CMake config files

# Parse the COMPONENTS from the original find_package call
set(_boost_components)
set(_boost_required)
set(_parsing_components FALSE)

foreach(_arg IN LISTS ARGN)
  if(_arg STREQUAL "REQUIRED")
    set(_boost_required "REQUIRED")
  elseif(_arg STREQUAL "COMPONENTS")
    set(_parsing_components TRUE)
  elseif(_parsing_components)
    list(APPEND _boost_components ${_arg})
  endif()
endforeach()

# Call Boost's config-based find_package
if(_boost_components)
  find_package(Boost ${_boost_required} CONFIG COMPONENTS ${_boost_components})
else()
  find_package(Boost ${_boost_required} CONFIG)
endif()
