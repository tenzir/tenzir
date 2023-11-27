find_library(
  FLUENT_BIT_LIB
  HINTS /opt/fluent/lib /opt/fluent-bit/lib
  NAMES fluent-bit
  PATH_SUFFIXES fluent-bit)

# We bring our own definitions, so we can ignore headers here.

get_filename_component(FLUENT_BIT_LOCATION ${FLUENT_BIT_LIB} DIRECTORY)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(fluentbit REQUIRED_VARS FLUENT_BIT_LIB
                                                          FLUENT_BIT_LOCATION)

mark_as_advanced(fluentbit_FOUND FLUENT_BIT_LIB FLUENT_BIT_LOCATION)

if (NOT TARGET fluentbit::fluentbit)
  add_library(fluentbit::fluentbit UNKNOWN IMPORTED)
  set_target_properties(
    fluentbit::fluentbit
    PROPERTIES IMPORTED_LOCATION "${FLUENT_BIT_LIB}" INTERFACE_LINK_DIRECTORIES
                                                     "${FLUENT_BIT_LOCATION}")
  if ("${FLUENT_BIT_LIB}" MATCHES "${CMAKE_STATIC_LIBRARY_SUFFIX}$")
    # The target dependencies need to be aligned with how the provided library
    # was built. In this case we assume the fluent-bit package from our Nix
    # scaffold.
    set(_link_libs)
    find_package(PkgConfig)
    pkg_check_modules(PC_YAML REQUIRED IMPORTED_TARGET yaml-0.1)
    list(APPEND _link_libs PkgConfig::PC_YAML)
    if (NOT APPLE) # We should really have a check for musl here.
      pkg_check_modules(PC_MUSL_FTS REQUIRED IMPORTED_TARGET musl-fts)
      list(APPEND _link_libs PkgConfig::PC_MUSL_FTS)
    endif ()
    if (APPLE)
      list(APPEND _link_libs resolv "-framework IOKit")
    endif ()
    set_property(
      TARGET fluentbit::fluentbit
      APPEND
      PROPERTY INTERFACE_LINK_LIBRARIES "${_link_libs}")
    # Inject the vendored static libs.
    file(GLOB FLUENT_BIT_LIBS "${FLUENT_BIT_LOCATION}/*.a")
    set_property(
      TARGET fluentbit::fluentbit
      APPEND
      PROPERTY INTERFACE_LINK_LIBRARIES "${FLUENT_BIT_LIBS}")
  endif ()
endif ()
