# Tries to find Broker library and headers
#
# Usage of this module as follows:
#
#     find_package(Broker)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  BROKER_ROOT_DIR           Set this variable to the root installation of
#                            Broker if the module has problems finding the
#                            proper installation path.
#
# Variables defined by this module:
#
#  BROKER_FOUND              System has Broker library
#  BROKER_LIBRARY            The broker library
#  BROKER_INCLUDE_DIRS       The broker headers

find_library(BROKER_LIBRARY
  NAMES broker
  HINTS ${BROKER_ROOT_DIR}/lib
        ${BROKER_ROOT_DIR})

find_path(BROKER_INCLUDE_DIRS
  NAMES broker/broker.hh
  HINTS ${BROKER_ROOT_DIR}/include
        ${BROKER_ROOT_DIR}/..)

if (BROKER_INCLUDE_DIRS)
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
  Broker
  DEFAULT_MSG
  BROKER_LIBRARY
  BROKER_INCLUDE_DIR)

mark_as_advanced(
  BROKER_ROOT_DIR
  BROKER_LIBRARY
  BROKER_INCLUDE_DIR)
