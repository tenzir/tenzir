# Tries to find librdkafka headers and libraries
#
# Usage of this module as follows:
#
#     find_package(Rdkafka)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  RDKAFKA_ROOT_DIR  Set this variable to the root installation of
#                 librdkafka if the module has problems finding
#                 the proper installation path.
#
# Variables defined by this module:
#
#  RDKAFKA_FOUND              System has rdkafka libs/headers
#  RDKAFKA_C_LIBRARY          The librdkafka C library
#  RDKAFKA_CPP_LIBRARY        The librdkafka C++ library
#  RDKAFKA_LIBRARIES          The rdkafka libraries
#  RDKAFKA_INCLUDE_DIR        The location of rdkafka headers

find_path(RDKAFKA_INCLUDE_DIR
  NAMES librdkafka/rdkafka.h
        librdkafka/rdkafkacpp.h
  HINTS ${RDKAFKA_ROOT_DIR}/include)

find_library(RDKAFKA_C_LIBRARY
  NAMES rdkafka
  HINTS ${RDKAFKA_ROOT_DIR}/lib)

find_library(RDKAFKA_CPP_LIBRARY
  NAMES rdkafka++
  HINTS ${RDKAFKA_ROOT_DIR}/lib)

set(RDKAFKA_LIBRARIES ${RDKAFKA_C_LIBRARY} ${RDKAFKA_CPP_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  Rdkafka
  DEFAULT_MSG
  RDKAFKA_LIBRARIES
  RDKAFKA_INCLUDE_DIR)

mark_as_advanced(
  RDKAFKA_ROOT_DIR
  RDKAFKA_LIBRARIES
  RDKAFKA_INCLUDE_DIR)
