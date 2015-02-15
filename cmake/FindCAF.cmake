# Try to find CAF headers and library.
#
# Use this module as follows:
#
#     find_package(CAF)
#
# Variables used by this module (they can change the default behaviour and need
# to be set before calling find_package):
#
#  CAF_ROOT_DIR  Set this variable to the root installation of
#                CAF if the module has problems finding 
#                the proper installation path.
#
# Variables defined by this module:
#
#  CAF_FOUND              System has CAF headers and library
#  CAF_LIBRARIES          List of library files  for all components
#  CAF_INCLUDE_DIRS       List of include paths for all components
#  CAF_LIBRARY_$C         Library file for component $C
#  CAF_INCLUDE_DIR_$C     Include path for component $C

# iterate over user-defined components
foreach (comp ${CAF_FIND_COMPONENTS})
  # we use uppercase letters only for variable names
  string(TOUPPER "${comp}" UPPERCOMP)
  if ("${comp}" STREQUAL "core")
    set(HDRNAME "caf/all.hpp")
  else ()
    set(HDRNAME "caf/${comp}/all.hpp")
  endif ()
  # look for headers: give CMake hints where to find non-installed CAF versions
  # note that we look for the headers of each component individually: this is
  # necessary to support non-installed versions of CAF, i.e., accessing the
  # checked out "actor-framework" or "caf" directory structure directly;
  # also check whether CAF_ROOT_DIR is a source directory
  set(HDRHINT "${CAF_ROOT_DIR}/libcaf_${comp}")
  foreach(dir ".." "../.." "../../..")
    foreach(subdir "actor-framework" "caf")
      set(HDRHINT ${HDRHINT} "${dir}/${subdir}/libcaf_${comp}")
    endforeach()
  endforeach()
  find_path(CAF_INCLUDE_DIR_${UPPERCOMP}
            NAMES
              ${HDRNAME}
            HINTS
              ${CAF_ROOT_DIR}/include
              /usr/include
              /usr/local/include
              /opt/local/include
              /sw/include
              ${CMAKE_INSTALL_PREFIX}/include
              ${HDRHINT})
  mark_as_advanced(CAF_INCLUDE_DIR_${UPPERCOMP})
  if (NOT "${CAF_INCLUDE_DIR_${UPPERCOMP}}"
      STREQUAL "CAF_INCLUDE_DIR_${UPPERCOMP}-NOTFOUND")
    # mark as found (set back to false in case library cannot be found)
    set(CAF_${comp}_FOUND true)
    # add to CAF_INCLUDE_DIRS only if path isn't already set
    set(duplicate false)
    foreach (p ${CAF_INCLUDE_DIRS})
      if (${p} STREQUAL ${CAF_INCLUDE_DIR_${UPPERCOMP}})
        set(duplicate true)
      endif ()
    endforeach ()
    if (NOT duplicate)
      set(CAF_INCLUDE_DIRS ${CAF_INCLUDE_DIRS} ${CAF_INCLUDE_DIR_${UPPERCOMP}})
    endif()
    # look for (.dll|.so|.dylib) file, again giving hints for non-installed CAFs
    # skip probe_event as it is header only
    if (NOT ${comp} STREQUAL "probe_event")
      unset(LIBHINT)
      foreach(dir ".." "../.." "../../..")
        foreach(subdir "actor-framework" "caf")
          set(LIBHINT ${LIBHINT} "${dir}/${subdir}/build/lib")
        endforeach()
      endforeach()
      find_library(CAF_LIBRARY_${UPPERCOMP}
                   NAMES
                     "caf_${comp}"
                   HINTS
                     ${CAF_ROOT_DIR}/lib
                     ${CAF_ROOT_DIR}/build/lib
                     /usr/lib
                     /usr/local/lib
                     /opt/local/lib
                     /sw/lib
                     ${CMAKE_INSTALL_PREFIX}/lib
                     ${LIBHINT})
      mark_as_advanced(CAF_LIBRARY_${UPPERCOMP})
      if ("${CAF_LIBRARY_${UPPERCOMP}}" STREQUAL "CAF_LIBRARY-NOTFOUND")
        set(CAF_${comp}_FOUND false)
      else ()
        set(CAF_LIBRARIES ${CAF_LIBRARIES} ${CAF_LIBRARY_${UPPERCOMP}})
      endif ()
    endif ()
  endif ()
endforeach ()

# let CMake check whether all requested components have been found
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CAF
                                  FOUND_VAR CAF_FOUND
                                  REQUIRED_VARS CAF_LIBRARIES CAF_INCLUDE_DIRS
                                  HANDLE_COMPONENTS)

# final step to tell CMake we're done
mark_as_advanced(CAF_ROOT_DIR
                 CAF_LIBRARIES
                 CAF_INCLUDE_DIRS)

