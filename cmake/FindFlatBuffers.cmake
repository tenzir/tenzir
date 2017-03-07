# Tries to find FlatBuffers.
#
# Usage of this module as follows:
#
#     find_package(FLATBUFFERS)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  FLATBUFFERS_ROOT_DIR  Set this variable to the root installation of
#                        FlatBuffers if the module has problems finding
#                        the proper installation path.
#
# Variables defined by this module:
#
#  FLATBUFFERS_FOUND          System has FlatBuffers compiler and includes
#  FLATBUFFERS_EXECUTABLE     The FlatBuffers compiler flatc
#  FLATBUFFERS_INCLUDE_DIR    The location of FlatBuffers headers


find_path(FLATBUFFERS_INCLUDE_DIR NAMES flatbuffers/flatbuffers.h)

find_program(FLATBUFFERS_EXECUTABLE
  NAMES flatc
  HINTS ${FLATBUFFERS_ROOT_DIR}/bin)

find_path(FLATBUFFERS_INCLUDE_DIR
  NAMES flatbuffers/flatbuffers.h
  HINTS ${FLATBUFFERS_ROOT_DIR}/include)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(flatbuffers
  DEFAULT_MSG FLATBUFFERS_EXECUTABLE FLATBUFFERS_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  FLATBUFFERS
  DEFAULT_MSG
  FLATBUFFERS_EXECUTABLE
  FLATBUFFERS_INCLUDE_DIR)

mark_as_advanced(
  FLATBUFFERS_ROOT_DIR
  FLATBUFFERS_EXECUTABLE
  FLATBUFFERS_INCLUDE_DIR)
