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
  template <typename T>
  using is_hashable = std::integral_constant<bool,
        std::is_arithmetic<T>::value || std::is_pod<T>::value>;

  /// Constructs a one-shot hash digest of a some bytes.
  template <typename Hack = Derived>
  static auto digest_bytes(void const* x, size_t n, uint32_t seed = 0)
    -> decltype(Hack::value(x, n, seed))
  {
    static_assert(std::is_same<Hack, Derived>::value, ":-P");
    return Derived::value(x, n, seed);
  }

  /// Constructs a one-shot hash digest of an object.
  template <
    typename T,
    typename Hack = Derived>
  static auto digest(T const& x, uint32_t seed = 0)
    -> decltype(is_hashable<T>{}, Hack::value(&x, sizeof(x), seed))
  {
    static_assert(std::is_same<Hack, Derived>::value, ":-P");
    return Derived::value(&x, sizeof(x), seed);
  }

  /// Adds data to an incremental hash computation.
  /// @tparam T A POD type.
  /// @param x The data to add.
  /// @return `true` on success.
  template <typename T>
  auto add(T const& x)
    -> std::enable_if_t<is_hashable<T>::value, bool>
  {
    return add(&x, sizeof(x));
  }

  /// Adds contiguous data to an incremental hash computation.
  /// @param x A pointer to the beginning of the data.
  /// @param n The number of bytes of *x*.
  /// @return `true` on success.
  bool add(void const* x, size_t n)
  {
    return static_cast<Derived*>(this)->update(x, n);
  }
  
  /// Retrieves the diegst of an incremental hash computation.
  /// @returns The hash digest according to the current state.
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
