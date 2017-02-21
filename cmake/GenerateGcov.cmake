# A simple script that invokes Gcov in the current, inspired by a much
# elaborate version from https://github.com/JoakimSoderberg/coveralls-cmake.
#
# This scripts expects the following variables:
#
# - GCOV_SOURCES: a list of files to generate coverage data for.
# - GCOV_OUTPUT_DIR: the directory where to write the *.gcov files to.

# Validate variables passed with -D.
if (NOT GCOV_SOURCES)
  message(FATAL_ERROR "Variable GCOV_SOURCES not provided")
endif ()

# Turn input back into a list.
separate_arguments(GCOV_SOURCES)

# Locate all *.gcda files in the current directory and invoke gcov on them.
# Thereafter, the resulting *.gcov files will reside in GCOV_OUTPUT_DIR.
file(GLOB_RECURSE GCDA_FILES "*.gcda")
if (NOT EXISTS ${GCOV_OUTPUT_DIR})
  file(MAKE_DIRECTORY ${GCOV_OUTPUT_DIR})
endif ()
message(STATUS "Generating *.gcov from *.gcda files")
foreach(GCDA ${GCDA_FILES})
	get_filename_component(GCDA_DIR ${GCDA} PATH)
	execute_process(
		COMMAND ${GCOV} -p -o ${GCDA_DIR} ${GCDA}
    WORKING_DIRECTORY "${GCOV_OUTPUT_DIR}"
    OUTPUT_QUIET
	)
endforeach()

message(STATUS "Weeding out unneeded *.gcov files")
file(GLOB GCOV_FILES "${GCOV_OUTPUT_DIR}/*.gcov")
set(CONSIDERED_GCOV_FILES)
foreach (GCOV_FILE ${GCOV_FILES})
  # Convert gcov filename /path/to/foo#bar#baz.ext.gcov to /foo/bar/baz.ext.
  get_filename_component(basename ${GCOV_FILE} NAME)
  string(REGEX REPLACE "\\.gcov$" "" SRC_FILE ${basename})
  string(REGEX REPLACE "\#" "/" SRC_FILE ${SRC_FILE})
  list(FIND GCOV_SOURCES ${SRC_FILE} FOUND)
  if (FOUND EQUAL -1)
    file(REMOVE ${GCOV_FILE}) # only keep relevant *.gcov files
	endif()
endforeach ()
