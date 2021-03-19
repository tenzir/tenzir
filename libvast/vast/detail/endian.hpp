//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstdint>

// Infer host endianness.
#if defined (__GLIBC__)
#  include <endian.h>
#  if (__BYTE_ORDER == __LITTLE_ENDIAN)
#    define VAST_BIG_ENDIAN 0
#    define VAST_LITTLE_ENDIAN 1
#  elif (__BYTE_ORDER == __BIG_ENDIAN)
#    define VAST_BIG_ENDIAN 1
#    define VAST_LITTLE_ENDIAN 0
#  else
#    error could not detect machine endianness
#  endif
#elif defined(_BIG_ENDIAN) && !defined(_LITTLE_ENDIAN) \
  || defined(__BIG_ENDIAN__) && !defined(__LITTLE_ENDIAN__) \
  || defined(_STLP_BIG_ENDIAN) && !defined(_STLP_LITTLE_ENDIAN)
#  define VAST_BIG_ENDIAN 1
#  define VAST_LITTLE_ENDIAN 0
#elif defined(_LITTLE_ENDIAN) && !defined(_BIG_ENDIAN) \
  || defined(__LITTLE_ENDIAN__) && !defined(__BIG_ENDIAN__) \
  || defined(_STLP_LITTLE_ENDIAN) && !defined(_STLP_BIG_ENDIAN)
#  define VAST_BIG_ENDIAN 0
#  define VAST_LITTLE_ENDIAN 1
#elif defined(__sparc) || defined(__sparc__) \
  || defined(_POWER) || defined(__powerpc__) \
  || defined(__ppc__) || defined(__hpux) || defined(__hppa) \
  || defined(_MIPSEB) || defined(_POWER) \
  || defined(__s390__)
#  define VAST_BIG_ENDIAN 1
#  define VAST_LITTLE_ENDIAN 0
#elif defined(__i386__) || defined(__alpha__) \
  || defined(__ia64) || defined(__ia64__) \
  || defined(_M_IX86) || defined(_M_IA64) \
  || defined(_M_ALPHA) || defined(__amd64) \
  || defined(__amd64__) || defined(_M_AMD64) \
  || defined(__x86_64) || defined(__x86_64__) \
  || defined(_M_X64) || defined(__bfin__)
#  define VAST_BIG_ENDIAN 0
#  define VAST_LITTLE_ENDIAN 1
#else
# error unsupported platform
#endif

namespace vast::detail {

/// Describes the two possible byte orders.
enum endianness {
  little_endian,
  big_endian
};

// The native endian of this machine.
#if VAST_LITTLE_ENDIAN
constexpr endianness host_endian = little_endian;
#elif VAST_BIG_ENDIAN
constexpr endianness host_endian = big_endian;
#endif

} // namespace vast::detail

