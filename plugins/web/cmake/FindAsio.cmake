# - Try to find asio
# Once done this will define the following variables:

#  ASIO_FOUND - System has libasio
#  ASIO_INCLUDE_DIRS - The libasio include directories

find_package(PkgConfig)

if (PkgConfig_FOUND)
  pkg_check_modules(PC_ASIO QUIET asio)

  find_path(ASIO_INCLUDE_DIRS asio.hpp HINTS ${PC_ASIO_INCLUDE_DIRS})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(Asio DEFAULT_MSG ASIO_INCLUDE_DIRS)
  mark_as_advanced(ASIO_FOUND ASIO_INCLUDE_DIRS)

  if (ASIO_FOUND)
    if (NOT TARGET Asio::Asio)
      add_library(Asio INTERFACE)
      target_include_directories(Asio INTERFACE ${ASIO_INCLUDE_DIRS})
      add_library(Asio::Asio ALIAS Asio)
    endif ()
  endif ()
endif ()
