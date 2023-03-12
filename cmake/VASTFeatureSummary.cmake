# -- feature summary -----------------------------------------------------------

include(FeatureSummary)

function (vast_feature_summary)
  # Append the feature summary to summary.log.
  feature_summary(
    WHAT ALL
    FILENAME "${CMAKE_CURRENT_BINARY_DIR}/summary.log"
    APPEND)

  # Print the feature and build summary if we're not a subproject.
  if (NOT VAST_IS_SUBPROJECT)
    feature_summary(WHAT ENABLED_FEATURES DISABLED_FEATURES
                    INCLUDE_QUIET_PACKAGES)
    unset(build_summary)
    list(APPEND build_summary "Build summary:\n")
    list(APPEND build_summary " * Edition: ${TENZIR_EDITION_NAME}")
    list(APPEND build_summary " * Version: ${VAST_VERSION_TAG}")
    list(APPEND build_summary " * Build Tree Hash: ${VAST_BUILD_TREE_HASH}")
    list(APPEND build_summary "")
    if (CMAKE_CONFIGURATION_TYPES)
      list(APPEND build_summary
           " * Configuration Types: ${CMAKE_CONFIGURATION_TYPES}")
    else ()
      list(APPEND build_summary " * Build Type: ${CMAKE_BUILD_TYPE}")
    endif ()
    list(APPEND build_summary " * Source Directory: ${CMAKE_SOURCE_DIR}")
    list(APPEND build_summary " * Binary Directory: ${CMAKE_BINARY_DIR}\n")
    list(APPEND build_summary " * System Name: ${CMAKE_SYSTEM_NAME}")
    list(APPEND build_summary " * CPU: ${CMAKE_SYSTEM_PROCESSOR}")
    foreach (lang IN ITEMS C CXX)
      set(_lang_compiler "${CMAKE_${lang}_COMPILER}")
      set(_lang_compiler_info
          "${CMAKE_${lang}_COMPILER_ID} ${CMAKE_${lang}_COMPILER_VERSION}")
      set(_lang_flags
          "${CMAKE_${lang}_FLAGS} ${CMAKE_${lang}_FLAGS_${CMAKE_BUILD_TYPE}}
        ${CMAKE_CPP_FLAGS} ${CMAKE_CPP_FLAGS_${CMAKE_BUILD_TYPE}}")
      string(STRIP "${_lang_flags}" _lang_flags)
      if (_lang_flags)
        list(
          APPEND
          build_summary
          " * ${lang} Compiler: ${_lang_compiler} (${_lang_compiler_info} with ${_lang_flags})"
        )
      else ()
        list(APPEND build_summary
             " * ${lang} Compiler: ${_lang_compiler} (${_lang_compiler_info})")
      endif ()
    endforeach ()
    list(APPEND build_summary " * Linker: ${CMAKE_LINKER}")
    list(APPEND build_summary " * Archiver: ${CMAKE_AR}")
    list(APPEND build_summary "")
    list(JOIN build_summary "\n" build_summary_joined)
    message(STATUS "${build_summary_joined}")
    get_property(VAST_DEPENDENCY_SUMMARY_CATEGORIES GLOBAL
                 PROPERTY "VAST_DEPENDENCY_SUMMARY_CATEGORIES_PROPERTY")
    foreach (category IN LISTS VAST_DEPENDENCY_SUMMARY_CATEGORIES)
      get_property(VAST_DEPENDENCY_SUMMARY GLOBAL
                   PROPERTY "VAST_DEPENDENCY_SUMMARY_${category}_PROPERTY")
      if (VAST_DEPENDENCY_SUMMARY)
        unset(build_summary)
        list(APPEND build_summary "${category}:")
        foreach (summary IN LISTS VAST_DEPENDENCY_SUMMARY)
          list(APPEND build_summary "${summary}")
        endforeach ()
        list(APPEND build_summary "")
        list(JOIN build_summary "\n" build_summary_joined)
        message(STATUS "${build_summary_joined}")
      endif ()
    endforeach ()
  endif ()
endfunction ()
