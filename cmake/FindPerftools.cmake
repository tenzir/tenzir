# - Try to find Perftools headers and libraries
#
# Usage of this module as follows:
#
#     find_package(Perftools)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Perftools_ROOT_DIR  Set this variable to the root installation of
#                      Perftools if the module has problems finding 
#                      the proper installation path.
#
# Variables defined by this module:
#
#  PERFTOOLS_FOUND              System has Perftools libs/headers
#  Perftools_LIBRARIES          The Perftools libraries
#  Perftools_INCLUDE_DIR        The location of Perftools headers

find_path(Perftools_ROOT_DIR
    NAMES include/google/heap-profiler.h
)

find_library(Perftools_LIBRARIES
    NAMES tcmalloc
    HINTS ${Perftools_ROOT_DIR}/lib
)

find_path(Perftools_INCLUDE_DIR
    NAMES google/heap-profiler.h
    HINTS ${Perftools_ROOT_DIR}/include
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Perftools DEFAULT_MSG
    Perftools_LIBRARIES
    Perftools_INCLUDE_DIR
)

mark_as_advanced(
    Perftools_ROOT_DIR
    Perftools_LIBRARIES
    Perftools_INCLUDE_DIR
)
