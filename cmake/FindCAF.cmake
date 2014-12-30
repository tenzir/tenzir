# Try to find CAF headers and library.
#
# Use this module as follows:
#
#     find_package(CAF)
#
# Variables used by this module (they can change the default behaviour and need
# to be set before calling find_package):
#
#  CAF_ROOT_DIR  Set this variable to the root installation of CAF if the
#                module has problems finding the proper installation path.
#
# Variables defined by this module:
#
#  CAF_FOUND              System has CAF headers and library
#  CAF_LIBRARIES          Location of the CAF library
#  CAF_INCLUDE_DIR        Location of the CAF headers

foreach (comp ${CAF_FIND_COMPONENTS})
  string(TOUPPER "${comp}" UPPERCOMP)
  if ("${comp}" STREQUAL "core")
    set(HDRNAME "caf/all.hpp")
  else ()
    set(HDRNAME "caf/${comp}/all.hpp")
  endif ()

  set(HDRHINT "actor-framework/libcaf_${comp}")
  unset(CAF_INCLUDE_DIR)
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
              ../${HDRHINT}
              ../../${HDRHINT}
              ../../../${HDRHINT})

  mark_as_advanced(CAF_INCLUDE_DIR_${UPPERCOMP})

  if ("${CAF_INCLUDE_DIR_${UPPERCOMP}}" STREQUAL "CAF_INCLUDE_DIR_${UPPERCOMP}-NOTFOUND")
    break ()
  else ()
    set(duplicate false)
    foreach (p ${CAF_INCLUDE_DIRS})
      if (${p} STREQUAL ${CAF_INCLUDE_DIR_${UPPERCOMP}})
        set(duplicate true)
      endif ()
    endforeach ()
    if (NOT duplicate)
      set(CAF_INCLUDE_DIRS ${CAF_INCLUDE_DIRS} ${CAF_INCLUDE_DIR_${UPPERCOMP}})
    endif()
  endif ()

  find_library(CAF_LIBRARY_${UPPERCOMP}
               NAMES
                 "caf_${comp}"
               HINTS
                 ${CAF_ROOT_DIR}/lib
                 /usr/lib
                 /usr/local/lib
                 /opt/local/lib
                 /sw/lib
                 ${CMAKE_INSTALL_PREFIX}/lib
                 ../actor-framework/build/lib
                 ../../actor-framework/build/lib
                 ../../../actor-framework/build/lib)

  mark_as_advanced(CAF_LIBRARY_${UPPERCOMP})

  if ("${CAF_LIBRARY_${UPPERCOMP}}" STREQUAL "CAF_LIBRARY-NOTFOUND")
    break ()
  else ()
    set(CAF_LIBRARIES ${CAF_LIBRARIES} ${CAF_LIBRARY_${UPPERCOMP}})
  endif ()
endforeach ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CAF DEFAULT_MSG
    CAF_LIBRARIES
    CAF_INCLUDE_DIRS)

mark_as_advanced(
    CAF_ROOT_DIR
    CAF_LIBRARIES
    CAF_INCLUDE_DIRS)
