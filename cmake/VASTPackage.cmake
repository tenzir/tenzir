# -- packaging -----------------------------------------------------------------

# We enforce that package trees are in the same location as install trees. This
# allows us to reuse artifacts with installation related substitutions in
# packages. This has the drawback that we can't use package specific prefixes.
if (NOT DEFINED CPACK_PACKAGING_INSTALL_PREFIX)
  set(CPACK_PACKAGING_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")
else ()
  if (NOT CPACK_PACKAGING_INSTALL_PREFIX STREQUAL CMAKE_INSTALL_PREFIX)
    message(WARNING "Mismatches between ${CPACK_PACKAGING_INSTALL_PREFIX} and \
        ${CMAKE_INSTALL_PREFIX} are known to produce broken packages")
  endif ()
endif ()
if (NOT DEFINED CPACK_PACKAGE_NAME)
  set(CPACK_PACKAGE_NAME "vast")
endif ()
set(CPACK_PACKAGE_VENDOR "Tenzir")
set(CPACK_PACKAGE_CONTACT "engineering@tenzir.com")
string(REGEX REPLACE "^v" "" CPACK_PACKAGE_VERSION "${VAST_VERSION_SHORT}")
if (NOT DEFINED CPACK_PACKAGE_FILE_NAME)
  # CPACK_SYSTEM_NAME is empty when this is evaluated.
  string(TOLOWER "${CMAKE_SYSTEM_NAME}" system_name_lower)
  set(CPACK_PACKAGE_FILE_NAME
      "${CPACK_PACKAGE_NAME}-${VAST_VERSION_SHORT}-${system_name_lower}")
  unset(system_name_lower)
  if (VAST_PACKAGE_FILE_NAME_SUFFIX)
    string(APPEND CPACK_PACKAGE_FILE_NAME "-${VAST_PACKAGE_FILE_NAME_SUFFIX}")
  endif ()
  set(CPACK_DEBIAN_FILE_NAME "DEB-DEFAULT")
endif ()
# Put packages into a dedicated package sub directory inside the build
# directory so we can find them a little easier.
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

# For the static binary builds it doesn't make much sense to install development
# utilities, so we never do so.
if (VAST_ENABLE_STATIC_EXECUTABLE)
  get_cmake_property(CPACK_COMPONENTS_ALL COMPONENTS)
  list(REMOVE_ITEM CPACK_COMPONENTS_ALL "Development" "Unspecified")
endif ()

# Create only a single package with all enabled install components rather than
# creating one package per install component that potentially duplicates the
# files. We should reconsider this in the future when we want to offer separate
# runtime and development Debian packages.
set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)

# Enable the component-based packages for the Debian and the Archive CPack
# generators. Sadly, this has to be done 1-by-1, and there does not appear to be
# an exhaustive list of generators that support this, so we only opt-in for the
# two common cases.
set(CPACK_DEBIAN_COMPONENT_INSTALL ON)
set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)

# Set up CPack as configured above. Note that the calls to cpack_add_component
# must come _after_ the CPack include, while the variables must be set _before_
# the include.
include(CPack)

# Define the grouping for packages based on the install components. For bundled
# dependencies in submodules the install component may be Unspecified, which we
# add as a disabled and hidden dependency of the enabled Development component.
cpack_add_component(
  Runtime
  DISPLAY_NAME "Runtime"
  DESCRIPTION "Runtime files for VAST"
  REQUIRED)
cpack_add_component(
  Unspecified
  DISPLAY_NAME "Unspecified"
  DISABLED)
cpack_add_component(
  Development
  DISPLAY_NAME "Development"
  DESCRIPTION "Development files for VAST"
  DEPENDS Unspecified Runtime)
