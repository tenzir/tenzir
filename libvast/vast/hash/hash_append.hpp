//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/bit.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/hash/uniquely_hashable.hpp"

#include <caf/detail/type_traits.hpp>
#include <caf/meta/save_callback.hpp>
#include <caf/meta/type_name.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace vast {

template <class HashAlgorithm, class T>
  requires(uniquely_hashable<T, HashAlgorithm>)
void hash_append(HashAlgorithm& h, const T& x) noexcept {
  h(std::addressof(x), sizeof(x));
}

namespace detail {

template <class T>
constexpr void reverse_bytes(T& x) {
  auto ptr = std::memmove(std::addressof(x), std::addressof(x), sizeof(T));
  auto bytes = static_cast<unsigned char*>(ptr);
  for (unsigned i = 0; i < sizeof(T) / 2; ++i)
    std::swap(bytes[i], bytes[sizeof(T) - 1 - i]);
}

template <class HashAlgorithm, class Container>
void contiguous_container_hash_append(HashAlgorithm& h,
                                      const Container& xs) noexcept {
  using value_type = typename Container::value_type;
  if constexpr (uniquely_hashable<value_type, HashAlgorithm>)
    h(std::data(xs), std::size(xs) * sizeof(value_type));
  else
    for (const auto& x : xs)
      hash_append(h, x);
  hash_append(h, std::size(xs));
}

} // namespace detail

// -- Scalars -----------------------------------------------------------------

template <class HashAlgorithm, class T>
  requires(!uniquely_hashable<T, HashAlgorithm> && std::is_scalar_v<T>)
void hash_append(HashAlgorithm& h, T x) noexcept {
  if constexpr (std::is_integral_v<
                  T> || std::is_pointer_v<T> || std::is_enum_v<T>) {
    detail::reverse_bytes(x);
    h(std::addressof(x), sizeof(x));
  } else if constexpr (std::is_floating_point_v<T>) {
    // When hashing, we treat -0 and 0 the same.
    if (x == 0)
      x = 0;
    if constexpr (HashAlgorithm::endian != detail::endian::native)
      detail::reverse_bytes(x);
    h(std::addressof(x), sizeof(x));
  } else {
    static_assert(std::is_same_v<T, T>, "T is neither integral nor a float");
  }
}

template <class HashAlgorithm>
void hash_append(HashAlgorithm& h, std::nullptr_t) noexcept {
  const void* p = nullptr;
  if constexpr (HashAlgorithm::endian != detail::endian::native)
    detail::reverse_bytes(p);
  h(std::addressof(p), sizeof(p));
}

// -- chrono ------------------------------------------------------------------

template <class HashAlgorithm, class Rep, class Period>
void hash_append(HashAlgorithm& h, std::chrono::duration<Rep, Period> d) {
  hash_append(h, d.count());
}

template <class HashAlgorithm, class Clock, class Duration>
void hash_append(HashAlgorithm& h, std::chrono::time_point<Clock, Duration> t) {
  hash_append(h, t.time_since_epoch());
}

// -- empty types -------------------------------------------------------------

template <class HashAlgorithm, class T>
  requires(std::is_empty_v<T>)
void hash_append(HashAlgorithm& h, T) noexcept {
  hash_append(h, 0);
}

// -- forward declarations to enable ADL --------------------------------------

template <class HashAlgorithm, class T, size_t N>
  requires(!uniquely_hashable<T, HashAlgorithm>)
void hash_append(HashAlgorithm& h, T (&a)[N]) noexcept;

template <class HashAlgorithm, class CharT, class Traits>
void hash_append(HashAlgorithm& h,
                 std::basic_string_view<CharT, Traits> s) noexcept;

template <class HashAlgorithm, class CharT, class Traits, class Alloc>
void hash_append(HashAlgorithm& h,
                 const std::basic_string<CharT, Traits, Alloc>& s) noexcept;

template <class HashAlgorithm, class T, class U>
  requires(!uniquely_hashable<std::pair<T, U>, HashAlgorithm>)
void hash_append(HashAlgorithm& h, const std::pair<T, U>& p) noexcept;

template <class HashAlgorithm, class T, size_t N>
  requires(!uniquely_hashable<std::array<T, N>, HashAlgorithm>)
void hash_append(HashAlgorithm& h, const std::array<T, N>& a) noexcept;

template <class HashAlgorithm, class T, class Alloc>
void hash_append(HashAlgorithm& h, const std::vector<T, Alloc>& v) noexcept;

template <class HashAlgorithm, class T, size_t Extent>
void hash_append(HashAlgorithm& h, std::span<T, Extent> xs) noexcept;

template <class HashAlgorithm, class Key, class Comp, class Alloc>
void hash_append(HashAlgorithm& h,
                 const std::set<Key, Comp, Alloc>& s) noexcept;

template <class HashAlgorithm, class Key, class T, class Comp, class Alloc>
void hash_append(HashAlgorithm& h,
                 const std::map<Key, T, Comp, Alloc>& m) noexcept;

template <class HashAlgorithm, class Key, class Hash, class Eq, class Alloc>
void hash_append(HashAlgorithm& h,
                 const std::unordered_set<Key, Hash, Eq, Alloc>& s) noexcept;

template <class HashAlgorithm, class K, class T, class Hash, class Eq,
          class Alloc>
void hash_append(HashAlgorithm& h,
                 const std::unordered_map<K, T, Hash, Eq, Alloc>& m) noexcept;

template <class HashAlgorithm, class... T>
  requires(!uniquely_hashable<std::tuple<T...>, HashAlgorithm>)
void hash_append(HashAlgorithm& h, const std::tuple<T...>& t) noexcept;

template <class HashAlgorithm, class T0, class T1, class... T>
void hash_append(HashAlgorithm& h, const T0& t0, const T1& t1,
                 const T&... t) noexcept;

// -- C array -----------------------------------------------------------------

template <class HashAlgorithm, class T, size_t N>
  requires(!uniquely_hashable<T, HashAlgorithm>)
void hash_append(HashAlgorithm& h, T (&a)[N]) noexcept {
  for (const auto& x : a)
    hash_append(h, x);
}

// -- string ------------------------------------------------------------------

template <class HashAlgorithm, class CharT, class Traits>
void hash_append(HashAlgorithm& h,
                 std::basic_string_view<CharT, Traits> xs) noexcept {
  detail::contiguous_container_hash_append(h, xs);
}

template <class HashAlgorithm, class CharT, class Traits, class Alloc>
void hash_append(HashAlgorithm& h,
                 const std::basic_string<CharT, Traits, Alloc>& xs) noexcept {
  detail::contiguous_container_hash_append(h, xs);
}

// -- pair --------------------------------------------------------------------

template <class HashAlgorithm, class T, class U>
  requires(!uniquely_hashable<std::pair<T, U>, HashAlgorithm>)
void hash_append(HashAlgorithm& h, const std::pair<T, U>& p) noexcept {
  hash_append(h, p.first, p.second);
}

// -- array -------------------------------------------------------------------

template <class HashAlgorithm, class T, size_t N>
  requires(!uniquely_hashable<std::array<T, N>, HashAlgorithm>)
void hash_append(HashAlgorithm& h, const std::array<T, N>& a) noexcept {
  for (const auto& t : a)
    hash_append(h, t);
}

// -- vector ------------------------------------------------------------------

template <class HashAlgorithm, class T, class Alloc>
void hash_append(HashAlgorithm& h, const std::vector<T, Alloc>& xs) noexcept {
  detail::contiguous_container_hash_append(h, xs);
}

// -- span ------------------------------------------------------------------

template <class HashAlgorithm, class T, size_t Extent>
void hash_append(HashAlgorithm& h, std::span<T, Extent> xs) noexcept {
  if constexpr (Extent == std::dynamic_extent) {
    detail::contiguous_container_hash_append(h, xs);
  } else if constexpr (Extent > 0) {
    // Just hash the data because the size is part of the type.
    if constexpr (uniquely_hashable<T, HashAlgorithm>)
      h(xs.data(), Extent * sizeof(T));
    else
      for (const auto& x : xs)
        hash_append(h, x);
  } else {
    // Do nothing for static empty spans, i.e., when Extent == 0.
  }
}

// -- set ---------------------------------------------------------------------

template <class HashAlgorithm, class Key, class Comp, class Alloc>
void hash_append(HashAlgorithm& h,
                 const std::set<Key, Comp, Alloc>& s) noexcept {
  for (const auto& x : s)
    hash_append(h, x);
  hash_append(h, s.size());
}

// -- map ---------------------------------------------------------------------

template <class HashAlgorithm, class Key, class T, class Comp, class Alloc>
void hash_append(HashAlgorithm& h,
                 const std::map<Key, T, Comp, Alloc>& m) noexcept {
  for (const auto& x : m)
    hash_append(h, x);
  hash_append(h, m.size());
}

// -- unordered_set -----------------------------------------------------------

template <class HashAlgorithm, class Key, class Hash, class Eq, class Alloc>
void hash_append(HashAlgorithm& h,
                 const std::unordered_set<Key, Hash, Eq, Alloc>& s) noexcept {
  for (const auto& x : s)
    hash_append(h, x);
  hash_append(h, s.size());
}

// -- unordered_map -----------------------------------------------------------

template <class HashAlgorithm, class K, class T, class Hash, class Eq,
          class Alloc>
void hash_append(HashAlgorithm& h,
                 const std::unordered_map<K, T, Hash, Eq, Alloc>& m) noexcept {
  for (const auto& x : m)
    hash_append(h, x);
  hash_append(h, m.size());
}

// -- tuple -------------------------------------------------------------------

template <class HashAlgorithm, class... T>
  requires(!uniquely_hashable<std::tuple<T...>, HashAlgorithm>)
void hash_append(HashAlgorithm& h, const std::tuple<T...>& t) noexcept {
  std::apply(
    [&h](auto&&... xs) {
      hash_append(h, std::forward<decltype(xs)>(xs)...);
    },
    t);
}

// -- variadic ----------------------------------------------------------------

template <class HashAlgorithm, class T0, class T1, class... Ts>
void hash_append(HashAlgorithm& h, const T0& x0, const T1& x1,
                 const Ts&... xs) noexcept {
  hash_append(h, x0);
  hash_append(h, x1, xs...);
}

// -- inspectable -------------------------------------------------------------

namespace detail {

template <class HashAlgorithm>
struct hash_inspector {
  using result_type = void;

  static constexpr bool reads_state = true;

  hash_inspector(HashAlgorithm& h) : h_{h} {
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
    requires(caf::meta::is_annotation<T>::value)
  result_type operator()(T&&, Ts&&... xs) const noexcept {
    (*this)(std::forward<Ts>(xs)...); // Ignore annotation.
  }

  template <class T, class... Ts>
    requires(!caf::meta::is_annotation<T>::value)
  result_type operator()(T&& x, Ts&&... xs) const noexcept {
    hash_append(h_, x);
    (*this)(std::forward<Ts>(xs)...);
  }

  HashAlgorithm& h_;
};

} // namespace detail

template <class HashAlgorithm, class T>
  requires(
    caf::detail::is_inspectable<detail::hash_inspector<HashAlgorithm>, T>::value
    && !uniquely_represented<T>)
void hash_append(HashAlgorithm& h, const T& x) noexcept {
  detail::hash_inspector<HashAlgorithm> f{h};
  inspect(f, const_cast<T&>(x));
}

} // namespace vast
