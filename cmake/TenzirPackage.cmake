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

# Tenzir replaces Tenzir, and that mechanism binds stronger than the package
# version, so we currently don't need EPOCHs. We still keep this setting in a
# commented out state here because it is ordering sensitive and having it
# simplifies a re-addition if it becomes necessary in the future.
#set(CPACK_DEBIAN_PACKAGE_EPOCH "1")

# Lowercase fits better for file names and such.
set(CPACK_PACKAGE_NAME "tenzir")
set(CPACK_PACKAGE_VENDOR "Tenzir")
set(CPACK_PACKAGE_CONTACT "engineering@tenzir.com")
string(REGEX REPLACE "^v" "" CPACK_PACKAGE_VERSION "${TENZIR_VERSION}")
if (NOT DEFINED CPACK_PACKAGE_FILE_NAME)
  # CPACK_SYSTEM_NAME is empty when this is evaluated.
  string(TOLOWER "${TENZIR_EDITION_NAME}" _edition_name_lower)
  string(TOLOWER "${CMAKE_SYSTEM_NAME}" _system_name_lower)
  set(CPACK_PACKAGE_FILE_NAME
      "${_edition_name_lower}-${TENZIR_VERSION}-${_system_name_lower}")
  unset(_system_name_lower)
  if (TENZIR_PACKAGE_FILE_NAME_SUFFIX)
    string(APPEND CPACK_PACKAGE_FILE_NAME "-${TENZIR_PACKAGE_FILE_NAME_SUFFIX}")
  endif ()
  if (NOT DEFINED CPACK_DEBIAN_FILE_NAME)
    # Taken from CPackDeb.cmake (BSD 3-Clause).
    # BEGIN SNIPPET
    find_program(DPKG_CMD dpkg)
    if (NOT DPKG_CMD)
      message(
        STATUS "TenzirPackage: Can not find dpkg in your path, default to i386."
      )
      set(_debian_package_architecture i386)
    endif ()
    execute_process(
      COMMAND "${DPKG_CMD}" --print-architecture
      OUTPUT_VARIABLE _debian_package_architecture
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    # END SNIPPET
    set(CPACK_DEBIAN_FILE_NAME
        "${_edition_name_lower}_${CPACK_PACKAGE_VERSION}_${_debian_package_architecture}.deb"
    )
    unset(_debian_package_architecture)
    unset(_debian_package_version)
    unset(_edition_name_lower)
  endif ()
endif ()

# Put packages into a dedicated package sub directory inside the build
# directory so we can find them a little easier.
set(CPACK_PACKAGE_DIRECTORY "package")
set(CPACK_VERBATIM_VARIABLES ON)

# TODO: Must be html files for productbuild.
if (NOT APPLE)
  set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
  set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")
endif ()
set(CPACK_INSTALLED_DIRECTORIES "/var/lib/tenzir" "/var/log/tenzir")

set(CPACK_DEBIAN_PACKAGE_RELEASE "1")
set(CPACK_DEBIAN_PACKAGE_RECOMMENDS "ca-certificates")
set(CPACK_DEBIAN_PACKAGE_DEPENDS "python3-venv")

set(CPACK_DEBIAN_COMPRESSION_TYPE "gzip")
set(CPACK_DEBIAN_PACKAGE_SECTION "contrib/database")
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    "${CMAKE_CURRENT_SOURCE_DIR}/scripts/debian/postinst"
    "${CMAKE_CURRENT_SOURCE_DIR}/scripts/debian/postrm"
    "${CMAKE_CURRENT_SOURCE_DIR}/scripts/debian/prerm")
set(CPACK_DEBIAN_PACKAGE_DEBUG ON)

set(CPACK_RPM_PRE_INSTALL_SCRIPT_FILE
  "${CMAKE_CURRENT_SOURCE_DIR}/scripts/rpm/preinstall")
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE
  "${CMAKE_CURRENT_SOURCE_DIR}/scripts/rpm/postinstall")
set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE
  "${CMAKE_CURRENT_SOURCE_DIR}/scripts/rpm/preuninstall")
set(CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE
  "${CMAKE_CURRENT_SOURCE_DIR}/scripts/rpm/postuninstall")
set(CPACK_RPM_PACKAGE_REQUIRES_PRE "shadow-utils")

# For the static binary builds it doesn't make much sense to install development
# utilities, so we never do so.
if (TENZIR_ENABLE_STATIC_EXECUTABLE)
  get_cmake_property(CPACK_COMPONENTS_ALL COMPONENTS)
  list(REMOVE_ITEM CPACK_COMPONENTS_ALL "Development" "Unspecified")
endif ()

# Create only a single package with all enabled install components rather than
# creating one package per install component that duplicates files because
# dependent packages are included in the installation for some CPack generators.
# We should reconsider this in the future when we want to offer separate runtime
# and development Debian packages.
set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)

# Enable the component-based packages for specified Generators.
# We only opt-in for the `DEB` and `ARCHIVE` Generators because those are the
# ones we support, and the list of available Generators can change with future
# releases of CMake.
# https://cmake.org/cmake/help/latest/module/CPackComponent.html#variable:CPACK_%3CGENNAME%3E_COMPONENT_INSTALL
set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_PRODUCTBUILD_COMPONENT_INSTALL ON)
set(CPACK_RPM_COMPONENT_INSTALL ON)

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
  DESCRIPTION "Runtime files for Tenzir"
  REQUIRED)
# TODO: Remove all Unspecified install components. The vendored fast_float
# library always installs itself; once fastfloat/fast_float#142 is merged we can
# update the vendored library and reconsider this.
cpack_add_component(
  Unspecified
  DISPLAY_NAME "Unspecified"
  HIDDEN)
cpack_add_component(
  Development
  DISPLAY_NAME "Development"
  DESCRIPTION "Development files for Tenzir"
  DEPENDS Unspecified Runtime)
