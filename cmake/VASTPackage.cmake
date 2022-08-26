# -- packaging -----------------------------------------------------------------

# We enforce that package trees are in the same location
# as install trees. This allows us to reuse artifacts with
# installation related substitutions in packages.
# This has the drawback that we can't use package specific
# prefixes.
if (NOT DEFINED CPACK_PACKAGING_INSTALL_PREFIX)
  set(CPACK_PACKAGING_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")
else ()
  if ("${CPACK_PACKAGING_INSTALL_PREFIX}" NOT STREQUAL
      "${CMAKE_INSTALL_PREFIX}")
    message(WARNING "Mismatches between ${CPACK_PACKAGING_INSTALL_PREFIX} and \
        ${CMAKE_INSTALL_PREFIX} are known to produce broken packages")
  endif ()
endif ()
if (NOT DEFINED CPACK_PACKAGE_NAME)
  set(CPACK_PACKAGE_NAME "vast")
endif ()
set(CPACK_PACKAGE_VENDOR "Tenzir")
set(CPACK_PACKAGE_CONTACT "engineering@tenzir.com")
set(CPACK_PACKAGE_VERSION "${VAST_VERSION_SHORT}")
if (NOT DEFINED CPACK_PACKAGE_FILE_NAME)
  # CPACK_SYSTEM_NAME is empty when this is evaluated.
  string(TOLOWER "${CMAKE_SYSTEM_NAME}" system_name_lower)
  set(CPACK_PACKAGE_FILE_NAME
      "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${system_name_lower}")
  unset(system_name_lower)
  if (VAST_PACKAGE_FILE_NAME_SUFFIX)
    string(APPEND CPACK_PACKAGE_FILE_NAME "-${VAST_PACKAGE_FILE_NAME_SUFFIX}")
  endif ()
  set(CPACK_DEBIAN_FILE_NAME "DEB-DEFAULT")
endif ()
# Put packages into a dedicated package sub directory inside the
# build directory so we can find them a little easier.
set(CPACK_PACKAGE_DIRECTORY "package")
set(CPACK_VERBATIM_VARIABLES ON)

set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
set(CPACK_INSTALLED_DIRECTORIES "/var/lib/vast" "/var/log/vast")

set(CPACK_DEBIAN_COMPRESSION_TYPE "gzip")
set(CPACK_DEBIAN_PACKAGE_SECTION "contrib/database")
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    "${CMAKE_CURRENT_SOURCE_DIR}/scripts/debian/postinst"
    "${CMAKE_CURRENT_SOURCE_DIR}/scripts/debian/postrm"
    "${CMAKE_CURRENT_SOURCE_DIR}/scripts/debian/prerm")
set(CPACK_DEBIAN_PACKAGE_DEBUG ON)

include(CPack)
