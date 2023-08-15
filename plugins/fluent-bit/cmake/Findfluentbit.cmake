find_library(
  FLUENT_BIT_LIB
  NAMES fluent-bit
  PATH_SUFFIXES fluent-bit)

find_path(FLUENT_BIT_INCLUDE_DIR fluent-bit.h)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(
  fluentbit REQUIRED_VARS FLUENT_BIT_LIB FLUENT_BIT_INCLUDE_DIR)

mark_as_advanced(fluentbit_FOUND FLUENT_BIT_INCLUDE_DIR FLUENT_BIT_LIB)

if (NOT TARGET fluentbit::fluentbit)
  add_library(fluentbit::fluentbit UNKNOWN IMPORTED)
  set_target_properties(
    fluentbit::fluentbit
    PROPERTIES IMPORTED_LOCATION "${FLUENT_BIT_LIB}"
               INTERFACE_INCLUDE_DIRECTORIES "${FLUENT_BIT_INCLUDE_DIR}"
               INTERFACE_LINK_LIBRARIES "${FLUENT_BIT_LIB}")
endif ()
