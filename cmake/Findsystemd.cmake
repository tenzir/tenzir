# - Try to find Systemd
# Once done this will define the following variables:

#  SYSTEMD_FOUND - System has libsystemd
#  SYSTEMD_INCLUDE_DIRS - The libsystemd include directories
#  SYSTEMD_LIBRARIES - The libraries needed to use libsystemd
#  SYSTEMD_DEFINITIONS - The libraries needed to use libsystemd

find_package(PkgConfig)

if (PkgConfig_FOUND)
	pkg_check_modules(PC_SYSTEMD QUIET libsystemd)

	find_library(SYSTEMD_LIBRARIES NAMES systemd ${PC_SYSTEMD_LIBRARY_DIRS})
	find_path(SYSTEMD_INCLUDE_DIRS systemd/sd-journal.h HINTS ${PC_SYSTEMD_INCLUDE_DIRS})
	set(SYSTEMD_DEFINITIONS ${PC_SYSTEMD_CFLAGS_OTHER})

	include(FindPackageHandleStandardArgs)
	find_package_handle_standard_args(systemd DEFAULT_MSG SYSTEMD_INCLUDE_DIRS SYSTEMD_LIBRARIES)
	mark_as_advanced(SYSTEMD_FOUND SYSTEMD_INCLUDE_DIRS SYSTEMD_LIBRARIES SYSTEMD_DEFINITIONS)

	if (SYSTEMD_FOUND)
	  message("systemd: ${systemd_LIBRARIES}")
	  if (NOT TARGET systemd::systemd)
	    add_library(systemd::systemd UNKNOWN IMPORTED)
	    set_target_properties(
	      systemd::systemd
	      PROPERTIES IMPORTED_LOCATION "${SYSTEMD_LIBRARIES}"
	                 INTERFACE_INCLUDE_DIRECTORIES "${SYSTEMD_INCLUDE_DIRS}"
	                 INTERFACE_LINK_LIBRARIES "${SYSTEMD_LIBRARIES}")
	  endif ()
	endif ()
endif ()
