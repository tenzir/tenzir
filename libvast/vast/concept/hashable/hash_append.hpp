/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_CONCEPT_HASHABLE_HASH_APPEND_HPP
#define VAST_CONCEPT_HASHABLE_HASH_APPEND_HPP

#include <cstddef>
#include <cstdint>
#include <cstring>

#include <array>
#include <chrono>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <caf/detail/type_traits.hpp>
#include <caf/meta/save_callback.hpp>
#include <caf/meta/type_name.hpp>

#include "vast/detail/endian.hpp"
#include "vast/detail/type_traits.hpp"

// In order to be hashable, a type T must satisfy at least one of the following
// requirements:
//
//     1. There exists an overload with the following signature:
//
//         template <class Hasher, class T>
//         void hash_append(Hasher& h, const T& x);
//
//     2. T is contiguously hashable under. That is, for all combinations of
//        two instances of T, say `x` and `y`, if `x == y`, then it must also
//        be true that `memcmp(addressof(x), addressof(y), sizeof(T)) == 0`.
//        That is, if `x == y`, then `x` and `y` have the same bit pattern
//        representation.
//
// See https://isocpp.org/files/papers/n3980.html for details.

namespace vast {
namespace detail {

// -- is_uniquely_represented ------------------------------------------------

template <class T>
struct is_uniquely_represented
  : std::integral_constant<bool, std::is_integral<T>{}
                                 || std::is_enum<T> {}
                                 || std::is_pointer<T>{}> {};

template <class T>
struct is_uniquely_represented<T const> : is_uniquely_represented<T> {};

template <class T, class U>
struct is_uniquely_represented<std::pair<T, U>>
  : std::integral_constant<
      bool,
      is_uniquely_represented<T>{}
        && is_uniquely_represented<U>{}
        && sizeof(T) + sizeof(U) == sizeof(std::pair<T, U>)
     > {};

template <class ...T>
struct is_uniquely_represented<std::tuple<T...>>
  : std::integral_constant<
      bool,
      std::conjunction<is_uniquely_represented<T>...>::value
        && detail::sum<sizeof(T)...>{} == sizeof(std::tuple<T...>)
    > {};

template <class T, size_t N>
struct is_uniquely_represented<T[N]> : is_uniquely_represented<T> {};

template <class T, size_t N>
struct is_uniquely_represented<std::array<T, N>>
  : std::integral_constant<
      bool,
      is_uniquely_represented<T>{} && sizeof(T) * N == sizeof(std::array<T, N>)
    > {};

// -- helpers ----------------------------------------------------------------

template <class T>
constexpr void reverse_bytes(T& x) {
  auto ptr = std::memmove(std::addressof(x), std::addressof(x), sizeof(T));
  unsigned char* bytes = static_cast<unsigned char*>(ptr);
  for (unsigned i = 0; i < sizeof(T) / 2; ++i)
    std::swap(bytes[i], bytes[sizeof(T) - 1 - i]);
}

template <class T, class Hasher>
void maybe_reverse_bytes(T& x, Hasher&) {
  if constexpr (Hasher::endian != host_endian)
    reverse_bytes(x);
}

// -- is_contiguously_hashable -----------------------------------------------

template <class T, class Hasher>
struct is_contiguously_hashable
  : std::integral_constant<
      bool,
      is_uniquely_represented<T>{}
        && (sizeof(T) == 1 || Hasher::endian == host_endian)
    > {};

template <class T, size_t N, class Hasher>
struct is_contiguously_hashable<T[N], Hasher>
  : std::integral_constant<
      bool,
      is_uniquely_represented<T[N]>{}
        && (sizeof(T) == 1 || Hasher::endian == host_endian)
    > {};

template <class T, class Hasher>
inline constexpr bool is_contiguously_hashable_v
  = is_contiguously_hashable<T, Hasher>::value;

} // namespace detail

template <class Hasher, class T>
std::enable_if_t<detail::is_contiguously_hashable<T, Hasher>{}>
hash_append(Hasher& h, const T& x) noexcept {
  h(std::addressof(x), sizeof(x));
}

// -- Scalars -----------------------------------------------------------------

template <class Hasher, class T>
std::enable_if_t<!detail::is_contiguously_hashable_v<T, Hasher>
                 && std::is_scalar_v<T>>
hash_append(Hasher& h, T x) noexcept {
  if constexpr (std::is_integral_v<T> || std::is_pointer_v<T>
                || std::is_enum_v<T>) {
    detail::reverse_bytes(x);
    h(std::addressof(x), sizeof(x));
  } else if constexpr (std::is_floating_point_v<T>) {
    // When hashing, we treat -0 and 0 the same.
    if (x == 0)
      x = 0;
    detail::maybe_reverse_bytes(x, h);
    h(&x, sizeof(x));
  } else {
    static_assert(std::is_same_v<T, T>, "T is neither integral nor a float");
  }
}

template <class Hasher>
void hash_append(Hasher& h, std::nullptr_t) noexcept {
  const void* p = nullptr;
  detail::maybe_reverse_bytes(p, h);
  h(&p, sizeof(p));
}

// -- chrono ------------------------------------------------------------------

template <class Hasher, class Rep, class Period>
void hash_append(Hasher& h, std::chrono::duration<Rep, Period> d) {
  hash_append(h, d.count());
}

template <class Hasher, class Clock, class Duration>
void hash_append(Hasher& h, std::chrono::time_point<Clock, Duration> t) {
  hash_append(h, t.time_since_epoch());
}

// -- empty types -------------------------------------------------------------

template <class Hasher, class T>
std::enable_if_t<std::is_empty<T>::value>
hash_append(Hasher& h, T) noexcept {
  hash_append(h, 0);
}

// -- forward declarations to enable ADL --------------------------------------

template <class Hasher, class T, size_t N>
std::enable_if_t<!detail::is_contiguously_hashable<T, Hasher>{}>
hash_append(Hasher& h, T (&a)[N]) noexcept;

template <class Hasher, class CharT, class Traits, class Alloc>
std::enable_if_t<!detail::is_contiguously_hashable<CharT, Hasher>{}>
hash_append(Hasher& h, const std::basic_string<CharT, Traits, Alloc>& s) noexcept;

template <class Hasher, class CharT, class Traits, class Alloc>
std::enable_if_t<detail::is_contiguously_hashable<CharT, Hasher>{}>
hash_append(Hasher& h, const std::basic_string<CharT, Traits, Alloc>& s) noexcept;

template <class Hasher, class T, class U>
std::enable_if_t<!detail::is_contiguously_hashable<std::pair<T, U>, Hasher>{}>
hash_append (Hasher& h, const std::pair<T, U>& p) noexcept;

template <class Hasher, class T, size_t N>
std::enable_if_t<!detail::is_contiguously_hashable<std::array<T, N>, Hasher>{}>
hash_append(Hasher& h, const std::array<T, N>& a) noexcept;

template <class Hasher, class T, class Alloc>
std::enable_if_t<!detail::is_contiguously_hashable<T, Hasher>{}>
hash_append(Hasher& h, const std::vector<T, Alloc>& v) noexcept;

template <class Hasher, class T, class Alloc>
std::enable_if_t<detail::is_contiguously_hashable<T, Hasher>{}>
hash_append(Hasher& h, const std::vector<T, Alloc>& v) noexcept;

template <class Hasher, class Key, class Comp, class Alloc>
void hash_append(Hasher& h, const std::set<Key, Comp, Alloc>& s) noexcept;

template <class Hasher, class Key, class T, class Comp, class Alloc>
void hash_append(Hasher& h, const std::map<Key, T, Comp, Alloc>& m) noexcept;

template <class Hasher, class Key, class Hash, class Eq, class Alloc>
void hash_append(Hasher& h,
                 const std::unordered_set<Key, Hash, Eq, Alloc>& s) noexcept;

template <class Hasher, class K, class T, class Hash, class Eq, class Alloc>
void hash_append(Hasher& h,
                 const std::unordered_map<K, T, Hash, Eq, Alloc>& m) noexcept;

template <class Hasher, class ...T>
std::enable_if_t<!detail::is_contiguously_hashable<std::tuple<T...>, Hasher>{}>
hash_append(Hasher& h, const std::tuple<T...>& t) noexcept;

template <class Hasher, class T0, class T1, class ...T>
void hash_append(Hasher& h, const T0& t0, const T1& t1, const T& ...t) noexcept;

// -- C array -----------------------------------------------------------------

template <class Hasher, class T, size_t N>
std::enable_if_t<!detail::is_contiguously_hashable<T, Hasher>{}>
hash_append(Hasher& h, T (&a)[N]) noexcept {
  for (const auto& x : a)
    hash_append(h, x);
}

// -- string ------------------------------------------------------------------

template <class Hasher, class CharT, class Traits, class Alloc>
std::enable_if_t<!detail::is_contiguously_hashable<CharT, Hasher>{} >
hash_append(Hasher& h,
            const std::basic_string<CharT, Traits, Alloc>& s) noexcept {
  for (auto c : s)
    hash_append(h, c);
  hash_append(h, s.size());
}

template <class Hasher, class CharT, class Traits, class Alloc>
std::enable_if_t<detail::is_contiguously_hashable<CharT, Hasher>{}>
hash_append(Hasher& h,
            const std::basic_string<CharT, Traits, Alloc>& s) noexcept {
  h(s.data(), s.size() * sizeof(CharT));
  hash_append(h, s.size());
}

// -- pair --------------------------------------------------------------------

template <class Hasher, class T, class U>
std::enable_if_t<!detail::is_contiguously_hashable<std::pair<T, U>, Hasher>{}>
hash_append(Hasher& h, const std::pair<T, U>& p) noexcept {
  hash_append(h, p.first, p.second);
}

// -- array -------------------------------------------------------------------

template <class Hasher, class T, size_t N>
std::enable_if_t<!detail::is_contiguously_hashable<std::array<T, N>, Hasher>{}>
hash_append(Hasher& h, const std::array<T, N>& a) noexcept {
  for (const auto& t : a)
    hash_append(h, t);
}

// -- vector ------------------------------------------------------------------

template <class Hasher, class T, class Alloc>
std::enable_if_t<detail::is_contiguously_hashable<T, Hasher>{}>
hash_append(Hasher& h, const std::vector<T, Alloc>& v) noexcept {
  h(v.data(), v.size() * sizeof(T));
  hash_append(h, v.size());
}

template <class Hasher, class T, class Alloc>
std::enable_if_t<!detail::is_contiguously_hashable<T, Hasher>{}>
hash_append(Hasher& h, const std::vector<T, Alloc>& v) noexcept {
  for (const auto& t : v)
    hash_append(h, t);
  hash_append(h, v.size());
}

// -- set ---------------------------------------------------------------------

template <class Hasher, class Key, class Comp, class Alloc>
void hash_append(Hasher& h, const std::set<Key, Comp, Alloc>& s) noexcept {
  for (const auto& x : s)
    hash_append(h, x);
  hash_append(h, s.size());
}

// -- map ---------------------------------------------------------------------

template <class Hasher, class Key, class T, class Comp, class Alloc>
void hash_append(Hasher& h, const std::map<Key, T, Comp, Alloc>& m) noexcept {
  for (const auto& x : m)
    hash_append(h, x);
  hash_append(h, m.size());
}

// -- unordered_set -----------------------------------------------------------

template <class Hasher, class Key, class Hash, class Eq, class Alloc>
void hash_append(Hasher& h,
                 const std::unordered_set<Key, Hash, Eq, Alloc>& s) noexcept {
  for (const auto& x : s)
    hash_append(h, x);
  hash_append(h, s.size());
}

// -- unordered_map -----------------------------------------------------------

template <class Hasher, class K, class T, class Hash, class Eq, class Alloc>
void hash_append(Hasher& h,
                 const std::unordered_map<K, T, Hash, Eq, Alloc>& m) noexcept {
  for (const auto& x : m)
    hash_append(h, x);
  hash_append(h, m.size());
}

// -- tuple -------------------------------------------------------------------

namespace detail {

inline void for_each_item(...) noexcept {
}

template <class Hasher, class T>
int hash_one(Hasher& h, const T& t) noexcept {
  hash_append(h, t);
  return 0;
}

template <class Hasher, class ...T, size_t ...I>
void tuple_hash(Hasher& h, const std::tuple<T...>& t,
                std::index_sequence<I...>) noexcept {
  for_each_item(hash_one(h, std::get<I>(t))...);
}

} // namespace detail

template <class Hasher, class ...T>
std::enable_if_t<!detail::is_contiguously_hashable<std::tuple<T...>, Hasher>{}>
hash_append(Hasher& h, const std::tuple<T...>& t) noexcept {
  detail::tuple_hash(h, t, std::index_sequence_for<T...>{});
}

// -- variadic ----------------------------------------------------------------

template <class Hasher, class T0, class T1, class... Ts>
void hash_append(Hasher& h, const T0& x0, const T1& x1,
                 const Ts& ...xs) noexcept {
  hash_append(h, x0);
  hash_append(h, x1, xs...);
}

// -- inspectable -------------------------------------------------------------

namespace detail {

template <class Hasher>
struct hash_inspector {
  using result_type = void;

  static constexpr bool reads_state = true;

  hash_inspector(Hasher& h) : h_{h} {
  }

  result_type operator()() const noexcept {
    // End of recursion.
  }

  template <class... Ts>
  result_type operator()(caf::meta::type_name_t x, Ts&&... xs) const {
    // Figure out the actual bytes to hash.
    auto ptr = x.value;
    while (*ptr != '\0')
      ++ptr;
    h_(x.value, ptr - x.value);
    (*this)(std::forward<Ts>(xs)...);
  }

  template <class F, class... Ts>
  result_type operator()(caf::meta::save_callback_t<F> x, Ts&&... xs) const {
    x.fun();
    (*this)(std::forward<Ts>(xs)...);
  }

  template <class T, class... Ts>
  std::enable_if_t<caf::meta::is_annotation<T>::value, result_type>
  operator()(T&&, Ts&&... xs) const noexcept {
    (*this)(std::forward<Ts>(xs)...); // Ignore annotation.
  }

  template <class T, class... Ts>
  std::enable_if_t<!caf::meta::is_annotation<T>::value, result_type>
  operator()(T&& x, Ts&&... xs) const noexcept {
    hash_append(h_, x);
    (*this)(std::forward<Ts>(xs)...);
  }

  Hasher& h_;
};

} // namespace detail

template <class Hasher, class T>
std::enable_if_t<
  caf::detail::is_inspectable<detail::hash_inspector<Hasher>, T>::value
>
hash_append(Hasher& h, const T& x) noexcept {
  detail::hash_inspector<Hasher> f{h};
  inspect(f, const_cast<T&>(x));
}

} // namespace vast

#endif
