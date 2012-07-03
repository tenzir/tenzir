# Tries to find Gperftools.
#
# Usage of this module as follows:
#
#     find_package(Gperftools)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Gperftools_ROOT_DIR  Set this variable to the root installation of
#                       Gperftools if the module has problems finding
#                       the proper installation path.
#
# Variables defined by this module:
#
#  Gperftools_FOUND              System has Gperftools libs/headers
#  Gperftools_LIBRARIES          The Gperftools libraries (tcmalloc & profiler)
#  Gperftools_INCLUDE_DIR        The location of Gperftools headers

find_path(Gperftools_ROOT_DIR NAMES include/google/heap-profiler.h)

find_library(Gperftools_tcmalloc
    NAMES tcmalloc
    HINTS ${Gperftools_ROOT_DIR}/lib)

find_library(Gperftools_profiler
    NAMES profiler
    HINTS ${Gperftools_ROOT_DIR}/lib)

find_library(Gperftools_tcmalloc_and_profiler
    NAMES tcmalloc_and_profiler
    HINTS ${Gperftools_ROOT_DIR}/lib)

find_path(Gperftools_INCLUDE_DIR
    NAMES Gperftools/heap-profiler.h
    HINTS ${Gperftools_ROOT_DIR}/include)

set(Gperftools_LIBRARIES ${Gperftools_tcmalloc_and_profiler})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    Gperftools
    DEFAULT_MSG
    Gperftools_LIBRARIES
    Gperftools_INCLUDE_DIR)

mark_as_advanced(
    Gperftools_ROOT_DIR
    Gperftools_tcmalloc
    Gperftools_profiler
    Gperftools_tcmalloc_and_profiler
    Gperftools_LIBRARIES
    Gperftools_INCLUDE_DIR)
