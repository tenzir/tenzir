find_library(
  FLUENT_BIT_LIB
  HINTS /opt/fluent/lib /opt/fluent-bit/lib
  NAMES fluent-bit
  PATH_SUFFIXES fluent-bit)

find_path(
  FLUENT_BIT_INCLUDE_DIR fluent-bit.h
  HINTS /opt/fluent/include /opt/fluent-bit/include)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(
  fluentbit REQUIRED_VARS FLUENT_BIT_INCLUDE_DIR FLUENT_BIT_LIB)

mark_as_advanced(fluentbit_FOUND FLUENT_BIT_INCLUDE_DIR FLUENT_BIT_LIB)

if (NOT TARGET fluentbit::fluentbit)
  add_library(fluentbit::fluentbit UNKNOWN IMPORTED)
  set_target_properties(
    fluentbit::fluentbit
    PROPERTIES IMPORTED_LOCATION "${FLUENT_BIT_LIB}"
               INTERFACE_INCLUDE_DIRECTORIES "${FLUENT_BIT_INCLUDE_DIR}")
  if(FLUENT_BIT_LIB MATCHES "${CMAKE_STATIC_LIBRARY_SUFFIX}$")
    # The target dependencies need to be aligned with how the provided library
    # was built. In this case we assume the fluent-bit package from our Nix
    # scaffold.
    find_package(PkgConfig)
    pkg_check_modules(PC_YAML REQUIRED IMPORTED_TARGET yaml-0.1)
    pkg_check_modules(PC_MUSL_FTS REQUIRED IMPORTED_TARGET musl-fts)
    set_property(TARGET
      fluentbit::fluentbit
      APPEND PROPERTY INTERFACE_LINK_LIBRARIES PkgConfig::PC_YAML PkgConfig::PC_MUSL_FTS)
    # Inject the vendored static libs.
    get_filename_component(dirname_ ${FLUENT_BIT_LIB} DIRECTORY)
    file(GLOB FLUENT_BIT_LIBS "${dirname_}/*.a")
    set_property(TARGET
      fluentbit::fluentbit
      APPEND PROPERTY INTERFACE_LINK_LIBRARIES "${FLUENT_BIT_LIBS}")
  endif ()
endif ()
