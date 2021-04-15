# Tries to find libpcap headers and libraries
#
# Usage of this module as follows:
#
# find_package(PCAP)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# PCAP_ROOT_DIR  Set this variable to the root installation of libpcap if the
# module has problems finding the proper installation path.
#
# Variables defined by this module:
#
# PCAP_FOUND              System has PCAP libs/headers PCAP_LIBRARIES The PCAP
# libraries PCAP_INCLUDE_DIR        The location of PCAP headers

find_path(
  PCAP_INCLUDE_DIR
  NAMES pcap.h
  HINTS ${PCAP_ROOT_DIR}/include)

find_library(
  PCAP_LIBRARIES
  NAMES pcap
  HINTS ${PCAP_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PCAP DEFAULT_MSG PCAP_LIBRARIES
                                  PCAP_INCLUDE_DIR)

mark_as_advanced(PCAP_ROOT_DIR PCAP_LIBRARIES PCAP_INCLUDE_DIR)

if (PCAP_FOUND)
  message(STATUS "Found libpcap: ${PCAP_LIBRARIES}")
endif ()

# create IMPORTED target for libpcap dependency
if (PCAP_FOUND AND NOT TARGET pcap::pcap)
  add_library(pcap::pcap UNKNOWN IMPORTED GLOBAL)
  set_target_properties(
    pcap::pcap PROPERTIES IMPORTED_LOCATION "${PCAP_LIBRARIES}"
                          INTERFACE_INCLUDE_DIRECTORIES "${PCAP_INCLUDE_DIR}")
endif ()

# TODO: Replace this with pkg_check_modules from the PkgConfig package, in case
# we add support for more vendor specific libpcap implementations.
if (NOT BUILD_SHARED_LIBS)
  execute_process(
    COMMAND "/bin/sh" "-c" "nm ${PCAP_LIBRARIES} | grep -q snf_init"
    RESULT_VARIABLE _result)
  if (NOT _result)
    set(PCAP_HAVE_SNF TRUE)
  else ()
    set(PCAP_HAVE_SNF FALSE)
  endif ()
endif ()

if (PCAP_HAVE_SNF)
  if (NOT SNF_ROOT_DIR)
    set(SNF_ROOT_DIR ${PCAP_ROOT_DIR})
  endif ()
  find_library(
    SNF_LIBRARIES
    NAMES libsnf.a
    HINTS ${SNF_ROOT_DIR}/lib)

  message(STATUS "Found Myricom SNF library: ${SNF_LIBRARIES}")
  if (SNF_LIBRARIES)
    if (NOT TARGET aria::snf)
      add_library(aria::snf STATIC IMPORTED GLOBAL)
      set_target_properties(aria::snf PROPERTIES IMPORTED_LOCATION
                                                 ${SNF_LIBRARIES})
    endif ()

    set_target_properties(pcap::pcap PROPERTIES INTERFACE_LINK_LIBRARIES
                                                "aria::snf")
  endif ()
endif ()
