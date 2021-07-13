# Check if SSE instructions are available on the machine where the project is
# compiled. This file is assembled from many posts in the CMake issue tracker
# at https://gitlab.kitware.com/cmake/cmake/-/issues.

include(CheckCXXCompilerFlag)

set(_CMAKE_REQUIRED_QUIET_OLD "${CMAKE_REQUIRED_QUIET}")
set(CMAKE_REQUIRED_QUIET "${SSE_FIND_QUIETLY}")
set(SSE2_FIND_QUIETLY "${SSE_FIND_QUIETLY}")
set(SSE3_FIND_QUIETLY "${SSE_FIND_QUIETLY}")
set(SSSE3_FIND_QUIETLY "${SSE_FIND_QUIETLY}")
set(SSE4_1_FIND_QUIETLY "${SSE_FIND_QUIETLY}")
set(SSE4_2_FIND_QUIETLY "${SSE_FIND_QUIETLY}")
set(AVX_FIND_QUIETLY "${SSE_FIND_QUIETLY}")
set(AVX2_FIND_QUIETLY "${SSE_FIND_QUIETLY}")

if (CMAKE_SYSTEM_NAME MATCHES "Linux"
    AND (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64" OR CMAKE_SYSTEM_PROCESSOR
                                                    MATCHES "i.86"))
  execute_process(COMMAND cat "/proc/cpuinfo" OUTPUT_VARIABLE CPUINFO)

  string(REGEX REPLACE "^.*(sse).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "sse" "${_SSE_THERE}" _SSE_TRUE)
  check_cxx_compiler_flag("-msse" _SSE_OK)

  string(REGEX REPLACE "^.*(sse2).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "sse2" "${_SSE_THERE}" _SSE2_TRUE)
  check_cxx_compiler_flag("-msse2" _SSE2_OK)

  string(REGEX REPLACE "^.*(pni).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "pni" "${_SSE_THERE}" _SSE3_TRUE)
  check_cxx_compiler_flag("-msse3" _SSE3_OK)

  string(REGEX REPLACE "^.*(ssse3).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "ssse3" "${_SSE_THERE}" _SSSE3_TRUE)
  check_cxx_compiler_flag("-mssse3" _SSSE3_OK)

  string(REGEX REPLACE "^.*(sse4_1).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "sse4_1" "${_SSE_THERE}" _SSE41_TRUE)
  check_cxx_compiler_flag("-msse4.1" _SSE41_OK)

  string(REGEX REPLACE "^.*(sse4_2).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "sse4_2" "${_SSE_THERE}" _SSE42_TRUE)
  check_cxx_compiler_flag("-msse4.2" _SSE42_OK)

  string(REGEX REPLACE "^.*(avx).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "avx" "${_SSE_THERE}" _AVX_TRUE)
  check_cxx_compiler_flag("-mavx" _AVX_OK)

  string(REGEX REPLACE "^.*(avx2).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "avx2" "${_SSE_THERE}" _AVX2_TRUE)
  check_cxx_compiler_flag("-mavx2" _AVX2_OK)
elseif (CMAKE_SYSTEM_NAME MATCHES "FreeBSD"
        AND (CMAKE_SYSTEM_PROCESSOR MATCHES "amd64" OR CMAKE_SYSTEM_PROCESSOR
                                                       MATCHES "i.86"))
  execute_process(
    COMMAND grep Features
    INPUT_FILE "/var/run/dmesg.boot"
    OUTPUT_VARIABLE CPUINFO)

  string(REGEX REPLACE "^.*(SSE).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSE" "${_SSE_THERE}" _SSE_TRUE)
  check_cxx_compiler_flag("-msse" _SSE_OK)

  string(REGEX REPLACE "^.*(SSE2).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSE2" "${_SSE_THERE}" _SSE2_TRUE)
  check_cxx_compiler_flag("-msse2" _SSE2_OK)

  string(REGEX REPLACE "^.*(SSE3).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSE3" "${_SSE_THERE}" _SSE3_TRUE)
  check_cxx_compiler_flag("-msse3" _SSE3_OK)

  string(REGEX REPLACE "^.*(SSSE3).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSSE3" "${_SSE_THERE}" _SSSE3_TRUE)
  check_cxx_compiler_flag("-mssse3" _SSSE3_OK)

  string(REGEX REPLACE "^.*(SSE4.1).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSE4.1" "${_SSE_THERE}" _SSE41_TRUE)
  check_cxx_compiler_flag("-msse4.1" _SSE41_OK)
  string(REGEX REPLACE "^.*(SSE4.2).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSE4.2" "${_SSE_THERE}" _SSE42_TRUE)
  check_cxx_compiler_flag("-msse4.2" _SSE42_OK)

  string(REGEX REPLACE "^.*(AVX).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "AVX" "${_SSE_THERE}" _AVX_TRUE)
  check_cxx_compiler_flag("-mavx" _AVX_OK)

  string(REGEX REPLACE "^.*(AVX2).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "AVX2" "${_SSE_THERE}" _AVX2_TRUE)
  check_cxx_compiler_flag("-mavx2" _AVX2_OK)
elseif (CMAKE_SYSTEM_NAME MATCHES "Darwin" AND NOT CMAKE_SYSTEM_PROCESSOR
                                               MATCHES "arm")
  execute_process(COMMAND sysctl -n machdep.cpu.features
                          machdep.cpu.leaf7_features OUTPUT_VARIABLE CPUINFO)

  string(REGEX REPLACE "^.*[^S](SSE).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSE" "${_SSE_THERE}" _SSE_TRUE)
  check_cxx_compiler_flag("-msse" _SSE_OK)

  string(REGEX REPLACE "^.*[^S](SSE2).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSE2" "${_SSE_THERE}" _SSE2_TRUE)
  check_cxx_compiler_flag("-msse2" _SSE2_OK)

  string(REGEX REPLACE "^.*[^S](SSE3).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSE3" "${_SSE_THERE}" _SSE3_TRUE)
  check_cxx_compiler_flag("-msse3" _SSE3_OK)

  string(REGEX REPLACE "^.*(SSSE3).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSSE3" "${_SSE_THERE}" _SSSE3_TRUE)
  check_cxx_compiler_flag("-mssse3" _SSSE3_OK)

  string(REGEX REPLACE "^.*(SSE4.1).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSE4.1" "${_SSE_THERE}" _SSE41_TRUE)
  check_cxx_compiler_flag("-msse4.1" _SSE41_OK)

  string(REGEX REPLACE "^.*(SSE4.2).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "SSE4.2" "${_SSE_THERE}" _SSE42_TRUE)
  check_cxx_compiler_flag("-msse4.2" _SSE42_OK)

  string(REGEX REPLACE "^.*(AVX).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "AVX" "${_SSE_THERE}" _AVX_TRUE)
  check_cxx_compiler_flag("-mavx" _AVX_OK)

  string(REGEX REPLACE "^.*(AVX2).*$" "\\1" _SSE_THERE "${CPUINFO}")
  string(COMPARE EQUAL "AVX2" "${_SSE_THERE}" _AVX2_TRUE)
  check_cxx_compiler_flag("-mavx2" _AVX2_OK)
else ()
  message(
    STATUS
      "FindSSE does not support ${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}."
  )
endif ()

set(CMAKE_REQUIRED_QUIET "${_CMAKE_REQUIRED_QUIET_OLD}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SSE REQUIRED_VARS _SSE_TRUE _SSE_OK)
set(FPHSA_NAME_MISMATCHED ON)
find_package_handle_standard_args(SSE2 REQUIRED_VARS _SSE2_TRUE _SSE2_OK)
find_package_handle_standard_args(SSE3 REQUIRED_VARS _SSE3_TRUE _SSE3_OK)
find_package_handle_standard_args(SSSE3 REQUIRED_VARS _SSSE3_TRUE _SSSE3_OK)
find_package_handle_standard_args(SSE4_1 REQUIRED_VARS _SSE41_TRUE _SSE41_OK)
find_package_handle_standard_args(SSE4_2 REQUIRED_VARS _SSE42_TRUE _SSE42_OK)
find_package_handle_standard_args(AVX REQUIRED_VARS _AVX_TRUE _AVX_OK)
find_package_handle_standard_args(AVX2 REQUIRED_VARS _AVX2_TRUE _AVX2_OK)
unset(FPHSA_NAME_MISMATCHED)

mark_as_advanced(
  SSE2_FOUND
  SSE3_FOUND
  SSSE3_FOUND
  SSE4_1_FOUND
  SSE4_2_FOUND
  AVX_FOUND
  AVX2_FOUND)

unset(_SSE_THERE)
unset(_SSE_TRUE)
unset(_SSE_OK)
unset(_SSE_OK CACHE)
unset(_SSE2_TRUE)
unset(_SSE2_OK)
unset(_SSE2_OK CACHE)
unset(_SSE3_TRUE)
unset(_SSE3_OK)
unset(_SSE3_OK CACHE)
unset(_SSSE3_TRUE)
unset(_SSSE3_OK)
unset(_SSSE3_OK CACHE)
unset(_SSE4_1_TRUE)
unset(_SSE41_OK)
unset(_SSE41_OK CACHE)
unset(_SSE4_2_TRUE)
unset(_SSE42_OK)
unset(_SSE42_OK CACHE)
unset(_AVX_TRUE)
unset(_AVX_OK)
unset(_AVX_OK CACHE)
unset(_AVX2_TRUE)
unset(_AVX2_OK)
unset(_AVX2_OK CACHE)
