#ifndef VAST_UTIL_HASH_H
#define VAST_UTIL_HASH_H

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace vast {
namespace util {

/// The base class for hash algorithms.
template <typename Derived>
struct hash
{
  template <typename Hack = Derived>
  static auto digest_bytes(void const* x, size_t n, uint32_t seed = 0)
    -> decltype(Hack::value(x, n, seed))
  {
    static_assert(std::is_same<Hack, Derived>::value, ":-P");
    return Derived::value(x, n, seed);
  }

  template <
    typename T,
    typename Hack = Derived>
  static auto digest(T const& x, uint32_t seed = 0)
    -> decltype(Hack::value(&x, sizeof(x), seed))
  {
    static_assert(std::is_same<Hack, Derived>::value, ":-P");
    return Derived::value(&x, sizeof(x), seed);
  }

  template <typename T>
  bool add(T const& x)
  {
    return add(&x, sizeof(x));
  }

  bool add(void const* x, size_t n)
  {
    return static_cast<Derived*>(this)->update(x, n);
  }
  
  template <typename Hack = Derived>
  auto get()
    -> decltype(std::declval<Hack>().compute())
  {
    static_assert(std::is_same<Hack, Derived>::value, ":-P");
    return static_cast<Derived*>(this)->compute();
  }
};

template <typename Hash, typename T>
typename Hash::digest_type digest(T&& x)
{
  Hash h;
  h.add(x);
  return h.get();
};

} // namespace util
} // namespace vast

#endif
