# Try to find libcaf headers and library.
#
# Use this module as follows:
#
#     find_package(Libcaf)
#
# Variables used by this module (they can change the default behaviour and need
# to be set before calling find_package):
#
#  LIBCAF_ROOT_DIR  Set this variable to the root installation of
#                   libcaf if the module has problems finding 
#                   the proper installation path.
#
# Variables defined by this module:
#
#  LIBCAF_FOUND              System has libcaf headers and library
#  LIBCAF_LIBRARIES          Location of the libcaf library
#  LIBCAF_INCLUDE_DIR        Location of the libcaf headers

foreach (comp ${Libcaf_FIND_COMPONENTS})
  string(TOUPPER "${comp}" UPPERCOMP)
  if ("${comp}" STREQUAL "core")
    set(HDRNAME "caf/all.hpp")
  else ()
    set(HDRNAME "caf/${comp}/all.hpp")
  endif ()

  set(HDRHINT "actor-framework/libcaf_${comp}")
  unset(LIBCAF_INCLUDE_DIR)
  find_path(LIBCAF_INCLUDE_DIR_${UPPERCOMP}
            NAMES
              ${HDRNAME}
            HINTS
              ${LIBCAF_ROOT_DIR}/include
              /usr/include
              /usr/local/include
              /opt/local/include
              /sw/include
              ${CMAKE_INSTALL_PREFIX}/include
              ../${HDRHINT}
              ../../${HDRHINT}
              ../../../${HDRHINT})

  mark_as_advanced(LIBCAF_INCLUDE_DIR_${UPPERCOMP})

  if ("${LIBCAF_INCLUDE_DIR_${UPPERCOMP}}" STREQUAL "LIBCAF_INCLUDE_DIR_${UPPERCOMP}-NOTFOUND")
    break ()
  else ()
    set(duplicate false)
    foreach (p ${LIBCAF_INCLUDE_DIRS})
      if (${p} STREQUAL ${LIBCAF_INCLUDE_DIR_${UPPERCOMP}})
        set(duplicate true)
      endif ()
    endforeach ()
    if (NOT duplicate)
      set(LIBCAF_INCLUDE_DIRS ${LIBCAF_INCLUDE_DIRS} ${LIBCAF_INCLUDE_DIR_${UPPERCOMP}})
    endif()
  endif ()

  find_library(LIBCAF_LIBRARY_${UPPERCOMP}
               NAMES
                 "caf_${comp}"
               HINTS
                 ${LIBCAF_ROOT_DIR}/lib
                 /usr/lib
                 /usr/local/lib
                 /opt/local/lib
                 /sw/lib
                 ${CMAKE_INSTALL_PREFIX}/lib
                 ../actor-framework/build/lib
                 ../../actor-framework/build/lib
                 ../../../actor-framework/build/lib)

  mark_as_advanced(LIBCAF_LIBRARY_${UPPERCOMP})

  if ("${LIBCAF_LIBRARY_${UPPERCOMP}}" STREQUAL "LIBCAF_LIBRARY-NOTFOUND")
    break ()
  else ()
    set(LIBCAF_LIBRARIES ${LIBCAF_LIBRARIES} ${LIBCAF_LIBRARY_${UPPERCOMP}})
  endif ()
endforeach ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libcaf DEFAULT_MSG
    LIBCAF_LIBRARIES
    LIBCAF_INCLUDE_DIRS)

mark_as_advanced(
    LIBCAF_ROOT_DIR
    LIBCAF_LIBRARIES
    LIBCAF_INCLUDE_DIRS)
