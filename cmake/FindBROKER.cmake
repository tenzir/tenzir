# Tries to find Broker library and headers
#
# Usage of this module as follows:
#
# find_package(Broker)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# BROKER_ROOT_DIR           Set this variable to the root installation of Broker
# if the module has problems finding the proper installation path.
#
# Variables defined by this module:
#
# BROKER_FOUND              System has Broker library BROKER_LIBRARY The broker
# library BROKER_INCLUDE_DIRS       The broker headers

find_library(
  BROKER_LIBRARY
  NAMES broker
  HINTS ${BROKER_ROOT_DIR}
  PATH_SUFFIXES lib)

find_path(
  BROKER_INCLUDE_DIRS
  NAMES broker/broker.hh
  HINTS ${BROKER_ROOT_DIR}
  PATH_SUFFIXES include .. ../..)

if (BROKER_INCLUDE_DIRS)
  # Extract the version.
  file(READ "${BROKER_INCLUDE_DIRS}/broker/version.hh" VERSION_HH)
  # get line containing the version
  string(REGEX MATCH " major = [0-9]+;" BROKER_VERSION_MAJOR "${VERSION_HH}")
  string(REGEX MATCH "[0-9]+" BROKER_VERSION_MAJOR "${BROKER_VERSION_MAJOR}")
  string(REGEX MATCH " minor = [0-9]+;" BROKER_VERSION_MINOR "${VERSION_HH}")
  string(REGEX MATCH "[0-9]+" BROKER_VERSION_MINOR "${BROKER_VERSION_MINOR}")
  string(REGEX MATCH " patch = [0-9]+;" BROKER_VERSION_PATCH "${VERSION_HH}")
  string(REGEX MATCH "[0-9]+" BROKER_VERSION_PATCH "${BROKER_VERSION_PATCH}")
  set(BROKER_VERSION
      "${BROKER_VERSION_MAJOR}.${BROKER_VERSION_MINOR}.${BROKER_VERSION_PATCH}")
  if (NOT BROKER_VERSION)
    unset(BROKER_VERSION)
    message(WARNING "Unable to determine Broker version")
  endif ()

  # When we're pointing to a build directory, we must add it to the include path
  # as well because it contains the broker/config.hh.
  set(broker_build_dir ${BROKER_LIBRARY})
  get_filename_component(broker_build_dir ${broker_build_dir} PATH)
  get_filename_component(broker_build_dir ${broker_build_dir} PATH)
  if (EXISTS "${broker_build_dir}/broker")
    set(BROKER_INCLUDE_DIRS ${BROKER_INCLUDE_DIRS} ${broker_build_dir})
  endif ()
endif ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  BROKER
  FOUND_VAR BROKER_FOUND
  REQUIRED_VARS BROKER_LIBRARY BROKER_INCLUDE_DIRS
  VERSION_VAR BROKER_VERSION)

mark_as_advanced(BROKER_LIBRARY BROKER_INCLUDE_DIRS)

# create IMPORTED target
if (BROKER_FOUND AND NOT TARGET zeek::broker)
  add_library(zeek::broker UNKNOWN IMPORTED GLOBAL)
  set_target_properties(
    zeek::broker
    PROPERTIES IMPORTED_LOCATION ${BROKER_LIBRARY}
               INTERFACE_INCLUDE_DIRECTORIES "${BROKER_INCLUDE_DIRS}"
               INTERFACE_LINK_LIBRARIES "CAF::core;CAF::io;CAF::openssl")
endif ()
