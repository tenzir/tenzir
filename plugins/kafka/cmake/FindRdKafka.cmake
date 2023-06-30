# Try to find RdKafka++.
#
# Once done this will define the following variables:
#  RdKafka_FOUND - System has librdkafka++
#  RdKafka_INCLUDE_DIRS - The librdkafka++ include directories
#  RdKafka_LIBRARIES - The libraries needed to use librdkafka++

find_package(PkgConfig)

if (PkgConfig_FOUND)
  pkg_check_modules(PC_RDKAFKA++ QUIET librdkafka++)

  find_library(
    RdKafka_LIBRARIES
    NAMES rdkafka++
    HINTS ${PC_RDKAFKA++_LIBRARY_DIRS})
  find_path(RdKafka_INCLUDE_DIRS librdkafka/rdkafkacpp.h
            HINTS ${PC_RDKAFKA++_INCLUDE_DIRS})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(RdKafka DEFAULT_MSG RdKafka_INCLUDE_DIRS
                                    RdKafka_LIBRARIES)
  mark_as_advanced(RdKafka_FOUND RdKafka_INCLUDE_DIRS RdKafka_LIBRARIES)

  if (RdKafka_FOUND)
    if (NOT TARGET RdKafka::rdkafka++)
      add_library(RdKafka::rdkafka++ UNKNOWN IMPORTED)
      set_target_properties(
        RdKafka::rdkafka++
        PROPERTIES IMPORTED_LOCATION "${RdKafka_LIBRARIES}"
                   INTERFACE_INCLUDE_DIRECTORIES "${RdKafka_INCLUDE_DIRS}"
                   INTERFACE_LINK_LIBRARIES "${RdKafka_LIBRARIES}")
    endif ()
  endif ()
endif ()
