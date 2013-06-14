#ifndef VAST_UTIL_BYTE_SWAP_H
#define VAST_UTIL_BYTE_SWAP_H

#include <cassert>
#include <type_traits>

#if defined (__GLIBC__)
#  include <endian.h>
#  if (__BYTE_ORDER == __LITTLE_ENDIAN)
#    define VAST_LITTLE_ENDIAN
#  elif (__BYTE_ORDER == __BIG_ENDIAN)
#    define VAST_BIG_ENDIAN
#  else
#    error could not detect machine endianness
#  endif
#elif defined(_BIG_ENDIAN) && !defined(_LITTLE_ENDIAN) || \
    defined(__BIG_ENDIAN__) && !defined(__LITTLE_ENDIAN__) || \
    defined(_STLP_BIG_ENDIAN) && !defined(_STLP_LITTLE_ENDIAN)
# define VAST_BIG_ENDIAN
#elif defined(_LITTLE_ENDIAN) && !defined(_BIG_ENDIAN) || \
    defined(__LITTLE_ENDIAN__) && !defined(__BIG_ENDIAN__) || \
    defined(_STLP_LITTLE_ENDIAN) && !defined(_STLP_BIG_ENDIAN)
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
# error this file needs to be set up for your CPU type.
#endif

namespace vast {

enum endianness
{
  little_endian,
  big_endian,
  network_endian = big_endian,
#if defined(VAST_LITTLE_ENDIAN)
  host_endian = little_endian
#elif defined(VAST_BIG_ENDIAN)
  host_endian = big_endian
#else
#error "unable to determine system endianness"
#endif
};

namespace util {
namespace detail {

template <typename T, size_t size>
struct swap_bytes
{
  inline T operator()(T)
  {
    assert(! "sizeof(T) is not 1, 2, 4, or 8");
  }
};

template <typename T>
struct swap_bytes<T, 1>
{
  inline T operator()(T x)
  {
    return x;
  }
};

template <typename T>
struct swap_bytes<T, 2>
{
  inline T operator()(T x)
  {
    return ((x >> 8) & 0xff) | ((x & 0xff) << 8);
  }
};

template<typename T>
struct swap_bytes<T, 4>
{
  inline T operator()(T x)
  {
    return ((x & 0xff000000) >> 24) |
           ((x & 0x00ff0000) >>  8) |
           ((x & 0x0000ff00) <<  8) |
           ((x & 0x000000ff) << 24);
  }
};

template <>
struct swap_bytes<float, 4>
{
  inline float operator()(float x)
  {
    uint32_t* ptr = reinterpret_cast<uint32_t*>(&x);
    uint32_t mem = swap_bytes<uint32_t, sizeof(uint32_t)>()(*ptr);
    float* f = reinterpret_cast<float*>(&mem);
    return *f;
  }
};

template <typename T>
struct swap_bytes<T, 8>
{
  inline T operator()(T x)
  {
    return ((x & 0xff00000000000000ull) >> 56) |
           ((x & 0x00ff000000000000ull) >> 40) |
           ((x & 0x0000ff0000000000ull) >> 24) |
           ((x & 0x000000ff00000000ull) >> 8 ) |
           ((x & 0x00000000ff000000ull) << 8 ) |
           ((x & 0x0000000000ff0000ull) << 24) |
           ((x & 0x000000000000ff00ull) << 40) |
           ((x & 0x00000000000000ffull) << 56);
  }
};

template <>
struct swap_bytes<double, 8>
{
  inline double operator()(double x)
  {
    uint64_t* ptr = reinterpret_cast<uint64_t*>(&x);
    uint64_t mem = swap_bytes<uint64_t, sizeof(uint64_t)>()(*ptr);
    double* d = reinterpret_cast<double*>(&mem);
    return *d;
  }
};

template <endianness from, endianness to, typename T>
struct dispatch_byte_swap
{
  inline T operator()(T x)
  {
    return swap_bytes<T, sizeof(T)>()(x);
  }
};

template <typename T>
struct dispatch_byte_swap<little_endian, little_endian, T>
{
  inline T operator()(T x)
  {
    return x;
  }
};

template <typename T>
struct dispatch_byte_swap<big_endian, big_endian, T>
{
  inline T operator()(T x)
  {
    return x;
  }
};

} // namespace detail

/// Changes the endianess of an arithmetic type. This function is essentially a
/// generic version combining @c hton* and @c ntoh*.
/// @tparam from The endianess to assume for and instance of T.
/// @tparam to The endianness to convert to.
/// @param x The value whose endianness to change.
template <endianness from, endianness to, typename T>
inline T byte_swap(T x)
{
  static_assert(std::is_arithmetic<T>::value, "T is no arithmetic type");

  static_assert(sizeof(T) == 1 ||
                sizeof(T) == 2 ||
                sizeof(T) == 4 ||
                sizeof(T) == 8,
                "sizeof(T) is not 1, 2, 4, or 8");

  return detail::dispatch_byte_swap<from, to, T>()(x);
}

} // namespace util
} // namespace vast

#endif
