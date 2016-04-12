#ifndef VAST_DETAIL_BYTE_SWAP_HPP
#define VAST_DETAIL_BYTE_SWAP_HPP

#include <cstdint>

// Infer host endianness.
#if defined (__GLIBC__)
#  include <endian.h>
#  if (__BYTE_ORDER == __LITTLE_ENDIAN)
#    define VAST_LITTLE_ENDIAN
#  elif (__BYTE_ORDER == __BIG_ENDIAN)
#    define VAST_BIG_ENDIAN
#  else
#    error could not detect machine endianness
#  endif
#elif defined(_BIG_ENDIAN) && !defined(_LITTLE_ENDIAN) \
  || defined(__BIG_ENDIAN__) && !defined(__LITTLE_ENDIAN__) \
  || defined(_STLP_BIG_ENDIAN) && !defined(_STLP_LITTLE_ENDIAN)
# define VAST_BIG_ENDIAN
#elif defined(_LITTLE_ENDIAN) && !defined(_BIG_ENDIAN) \
  || defined(__LITTLE_ENDIAN__) && !defined(__BIG_ENDIAN__) \
  || defined(_STLP_LITTLE_ENDIAN) && !defined(_STLP_BIG_ENDIAN)
# define VAST_LITTLE_ENDIAN
#elif defined(__sparc) || defined(__sparc__) \
  || defined(_POWER) || defined(__powerpc__) \
  || defined(__ppc__) || defined(__hpux) || defined(__hppa) \
  || defined(_MIPSEB) || defined(_POWER) \
  || defined(__s390__)
# define VAST_BIG_ENDIAN
#elif defined(__i386__) || defined(__alpha__) \
  || defined(__ia64) || defined(__ia64__) \
  || defined(_M_IX86) || defined(_M_IA64) \
  || defined(_M_ALPHA) || defined(__amd64) \
  || defined(__amd64__) || defined(_M_AMD64) \
  || defined(__x86_64) || defined(__x86_64__) \
  || defined(_M_X64) || defined(__bfin__)
# define VAST_LITTLE_ENDIAN
#else
# error unsupported platform
#endif

namespace vast {
namespace detail {

/// Describes the two possible byte orders.
enum endianness {
  little_endian,
  big_endian
};

#if defined(VAST_LITTLE_ENDIAN)
constexpr endianness host_endian = little_endian;
#elif defined(VAST_BIG_ENDIAN)
constexpr endianness host_endian = big_endian;
#endif

/// Swaps the endianness of an unsigned integer.
/// @param x The value whose bytes to swap.
/// @returns The value with swapped byte order.
/// @see to_network_order to_host_order
inline uint8_t byte_swap(uint8_t x) {
  return x;
}

inline uint16_t byte_swap(uint16_t x) {
  return __builtin_bswap16(x);
}

inline uint32_t byte_swap(uint32_t x) {
  return __builtin_bswap32(x);
}

inline uint64_t byte_swap(uint64_t x) {
  return __builtin_bswap64(x);
}

/// Converts the bytes of an unsigned integer from host order to network order.
/// @param x The value to convert.
/// @returns The value with bytes in network order.
/// @see endianness byte_swap to_host_order
template <class T>
T to_network_order(T x) {
#if defined(VAST_LITTLE_ENDIAN)
  return byte_swap(x);
#elif defined(VAST_BIG_ENDIAN)
  return x;
#endif
}

/// Converts the bytes of an unsigned integer from network order to host order.
/// @param x The value to convert.
/// @returns The value with bytes in host order.
/// @see endianness byte_swap to_network_order
template <class T>
T to_host_order(T x) {
  return to_network_order(x);
}

} // namespace detail
} // namespace vast

#endif
