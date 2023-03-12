# -- caf -----------------------------------------------------------------------

# The CAF dependency is loaded project-wide because both libvast and
# libvast_test need it.

# CAF::openssl needs OpenSSL, but CAFConfig.cmake does not pull it in.
find_package(OpenSSL REQUIRED)

# TODO: Require CAF to be installed.

if (VAST_ENABLE_DEVELOPER_MODE
    AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../libvast/aux/caf/CMakeLists.txt"
    AND NOT CAF_ROOT_DIR)
  set(VAST_ENABLE_BUNDLED_CAF_DEFAULT ON)
else ()
  set(VAST_ENABLE_BUNDLED_CAF_DEFAULT OFF)
endif ()
option(VAST_ENABLE_BUNDLED_CAF "Use the CAF submodule"
       "${VAST_ENABLE_BUNDLED_CAF_DEFAULT}")
add_feature_info("VAST_ENABLE_BUNDLED_CAF" VAST_ENABLE_BUNDLED_CAF
                 "use the CAF submodule.")
if (NOT VAST_ENABLE_BUNDLED_CAF)
  # Try to find the required CAF components first...
  find_package(
    CAF
    COMPONENTS core io test openssl
    REQUIRED)
  if (NOT ${CAF_VERSION} VERSION_GREATER_EQUAL 0.18.6)
    message(
      FATAL_ERROR "Failed to find CAF >= 0.18.6 version (found ${CAF_VERSION})")
  endif ()
  string(
    APPEND
    VAST_FIND_DEPENDENCY_LIST
    "\nfind_package(CAF ${CAF_VERSION} COMPONENTS core io test openssl REQUIRED)"
  )
  dependency_summary("CAF" CAF::core "Dependencies")
else ()
  # Use bundled CAF.
  if (NOT EXISTS
      "${CMAKE_CURRENT_SOURCE_DIR}/../libvast/aux/caf/CMakeLists.txt")
    message(
      FATAL_ERROR
        "CAF not found, either use -DCAF_ROOT_DIR=... or initialize the libvast/aux/caf submodule"
    )
  else ()
    set(VAST_ENABLE_BUNDLED_CAF ON)
    set(CAF_LOG_LEVEL ${VAST_CAF_LOG_LEVEL})
    set(CAF_ENABLE_EXAMPLES OFF)
    set(CAF_ENABLE_TESTING OFF)
    set(CAF_ENABLE_TOOLS OFF)
    set(CAF_ENABLE_OPENSSL ON)
    # add_subdirectory libvast/aux/caf checks if compiler supports the c++ 17. This check fails and the workaround
    # can be removed once CAF cmake changes to use set(CMAKE_CXX_STANDARD) instead of it's own check
    set(ORIGINAL_CXX_STANDARD ${CMAKE_CXX_STANDARD})
    set(CMAKE_CXX_STANDARD 17)
    add_subdirectory(libvast/aux/caf)
    set(CMAKE_CXX_STANDARD ${ORIGINAL_CXX_STANDARD})
    VASTSystemizeTarget(libcaf_core)
    set_target_properties(libcaf_core PROPERTIES EXPORT_NAME core)
    target_compile_features(libcaf_core INTERFACE cxx_std_17)
    target_compile_options(
      libcaf_core
      PRIVATE
        -Wno-maybe-uninitialized
        -Wno-unqualified-std-cast-call
        -Wno-unknown-warning-option
        $<$<CXX_COMPILER_ID:Clang,AppleClang>:-Wno-deprecated-declarations>
        $<$<CXX_COMPILER_ID:GNU>:-Wno-deprecated>)
    VASTSystemizeTarget(libcaf_io)
    set_target_properties(libcaf_io PROPERTIES EXPORT_NAME io)
    target_compile_options(
      libcaf_io PRIVATE -Wno-maybe-uninitialized -Wno-unqualified-std-cast-call
                        -Wno-unknown-warning-option)
    if (NOT TARGET libcaf_openssl)
      string(
        JOIN " " err_message
        "Unable to find the bundled CAF's OpenSSL module; consider setting "
        "OPENSSL_ROOT_DIR to the install prefix of your OpenSSL installation.")
      message(FATAL_ERROR "${err_message}")
    endif ()
    target_include_directories(
      libcaf_openssl
      PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../libvast/aux/caf/libcaf_openssl>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    VASTSystemizeTarget(libcaf_openssl)
    set_target_properties(libcaf_openssl PROPERTIES EXPORT_NAME openssl)
    VASTSystemizeTarget(libcaf_test)
    set_target_properties(libcaf_test PROPERTIES EXPORT_NAME test)
    mark_as_advanced(caf_build_header_path)
    string(APPEND VAST_EXTRA_TARGETS_FILES
           "\ninclude(\"\${CMAKE_CURRENT_LIST_DIR}/../CAF/CAFTargets.cmake\")"
           "\nmark_as_advanced(caf_build_header_path)")
    set(CAF_FOUND true)
  endif ()
  # Make bundled CAF available for component-based CPack installations.
  install(TARGETS libcaf_core libcaf_openssl libcaf_io COMPONENT Runtime)
  install(TARGETS libcaf_test COMPONENT Development)
  # Figure out whether we point to a build directory or a prefix.
  dependency_summary("CAF" "${CMAKE_CURRENT_SOURCE_DIR}/../libvast/aux/caf"
                     "Dependencies")
endif ()
