
if (NOT CAF_ROOT_DIR AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/caf/CMakeLists.txt)
  set(CAF_NO_AUTO_LIBCPP TRUE)
  set(CAF_NO_OPENCL TRUE)
  set(CAF_NO_EXAMPLES TRUE)
  set(CAF_NO_UNIT_TESTS TRUE)
  set(CAF_NO_PYTHON TRUE)
  set(CAF_NO_TOOLS TRUE)
  if (BUILD_SHARED_LIBS)
    set(_linkage_suffix shared)
  else ()
    set(_linkage_suffix static)
    set(CAF_BUILD_STATIC TRUE)
    set(CAF_BUILD_STATIC_ONLY TRUE)
  endif ()
  add_subdirectory(caf)
  set_target_properties(libcaf_core_${_linkage_suffix}
    PROPERTIES EXPORT_NAME core)
  set_property(TARGET libcaf_core_${_linkage_suffix} PROPERTY CXX_CLANG_TIDY)
  set_property(TARGET libcaf_core_${_linkage_suffix} PROPERTY CXX_INCLUDE_WHAT_YOU_USE)
  add_library(caf::core ALIAS libcaf_core_${_linkage_suffix})
  set_target_properties(libcaf_io_${_linkage_suffix}
    PROPERTIES EXPORT_NAME io)
  set_property(TARGET libcaf_io_${_linkage_suffix} PROPERTY CXX_CLANG_TIDY)
  set_property(TARGET libcaf_io_${_linkage_suffix} PROPERTY CXX_INCLUDE_WHAT_YOU_USE)
  add_library(caf::io ALIAS libcaf_io_${_linkage_suffix})
  install(TARGETS libcaf_core_${_linkage_suffix} libcaf_io_${_linkage_suffix}
      EXPORT CAFTargets
          ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
          LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
          RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  if (TARGET libcaf_openssl_${_linkage_suffix})
    target_include_directories(
      libcaf_openssl_${_linkage_suffix}
      PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/caf/libcaf_openssl>
             $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    set_target_properties(libcaf_openssl_${_linkage_suffix}
      PROPERTIES EXPORT_NAME openssl)
    set_property(TARGET libcaf_openssl_${_linkage_suffix} PROPERTY CXX_CLANG_TIDY)
    set_property(TARGET libcaf_openssl_${_linkage_suffix} PROPERTY CXX_INCLUDE_WHAT_YOU_USE)
    add_library(caf::openssl ALIAS libcaf_openssl_${_linkage_suffix})
    install(TARGETS libcaf_openssl_${_linkage_suffix}
        EXPORT CAFTargets
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
        set(VAST_USE_OPENSSL true PARENT_SCOPE)
  endif ()
  add_library(libcaf_test INTERFACE)
  target_link_libraries(libcaf_test INTERFACE caf::core)
  target_include_directories(libcaf_test
    INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/caf/libcaf_test>
              $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  set_target_properties(libcaf_test
    PROPERTIES EXPORT_NAME test)
  add_library(caf::test ALIAS libcaf_test)
  install(TARGETS libcaf_test
      EXPORT CAFTargets
          ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
          LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
          RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
      set(caf_dir ${CMAKE_CURRENT_SOURCE_DIR}/caf PARENT_SCOPE)
  export(EXPORT CAFTargets FILE CAFTargets.cmake NAMESPACE caf::)
  install(EXPORT CAFTargets
          DESTINATION "${INSTALL_VAST_CMAKEDIR}"
          NAMESPACE caf::)
  string(APPEND VAST_CAF_TARGETS_FILE
    "include(\"\${CMAKE_CURRENT_LIST_DIR}/CAFTargets.cmake\")")
else ()
  if (NOT CAF_ROOT_DIR AND VAST_PREFIX)
    set(CAF_ROOT_DIR ${VAST_PREFIX})
  endif ()
  if (CAF_ROOT_DIR)
    # Try to find the required CAF components first...
    find_package(CAF COMPONENTS core io test REQUIRED QUIET)
    message(STATUS "Found CAF")
    provide_find_module(CAF)
    string(APPEND VAST_FIND_DEPENDENCY_LIST
           "\nfind_package(CAF COMPONENTS core io test REQUIRED QUIET)")
    set(CAF_FOUND_SAVE ${CAF_FOUND})
    # ...and then optional components.
    find_package(CAF COMPONENTS openssl QUIET)
    if (CAF_LIBRARY_OPENSSL)
      message(STATUS "Found CAF's OpenSSL module, enabling OpenSSL support")
      set(VAST_USE_OPENSSL true PARENT_SCOPE)
      string(APPEND VAST_FIND_DEPENDENCY_LIST
             "\nfind_package(CAF COMPONENTS openssl REQUIRED QUIET)")
    endif ()
    set(CAF_FOUND ${CAF_FOUND_SAVE} PARENT_SCOPE)
  else ()
    message(FATAL_ERROR "CAF not found, either use -DCAF_ROOT_DIR=... or initialize the aux/caf submodule")
  endif ()
endif ()

if (ENABLE_ZEEK_TO_VAST)
  if (NOT BROKER_ROOT_DIR AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/broker/broker/CMakeLists.txt)
    add_subdirectory(broker)
    set(VAST_HAVE_BROKER true PARENT_SCOPE)
  else ()
    if (NOT BROKER_ROOT_DIR AND VAST_PREFIX)
      set(BROKER_ROOT_DIR "${VAST_PREFIX}")
    endif ()
    if (BROKER_ROOT_DIR)
      find_package(BROKER REQUIRED)
      set(VAST_HAVE_BROKER true PARENT_SCOPE)
    else ()
      message(FATAL_ERROR "Broker not found, either use -DBROKER_ROOT_DIR=... or initialize the aux/broker submodule")
    endif ()
  endif ()
endif ()
