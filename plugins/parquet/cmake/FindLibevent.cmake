find_package(PkgConfig)
if (PkgConfig_FOUND)
  pkg_check_modules(PC_Libevent Libevent)

  find_path(
    Libevent_INCLUDE_DIRS
    NAMES event.h
    HINTS ${PC_Libevent_INCLUDE_DIRS})

  find_library(
    Libevent_LIBRARIES
    NAMES event_core event_extra event_pthreads
    HINTS ${PC_Libevent_LIBRARY_DIRS})

  set(Libevent_VERSION ${PC_Libevent_VERSION})

  include(FindPackageHandleStandardArgs)

  find_package_handle_standard_args(
    Libevent
    REQUIRED_VARS Libevent_LIBRARIES Libevent_INCLUDE_DIRS
    VERSION_VAR Libevent_VERSION)

  mark_as_advanced(Libevent_INCLUDE_DIRS Libevent_LIBRARIES)

  if (Libevent_FOUND)
    if (NOT TARGET libevent::event)
      add_library(libevent::event INTERFACE IMPORTED GLOBAL)
      set_target_properties(
        libevent::event
        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${Libevent_INCLUDE_DIRS}"
                   INTERFACE_LINK_LIBRARIES "${Libevent_LIBRARIES}")
    endif ()
  endif ()
endif ()
