//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/detail/byteswap.hpp"

#include <cstdint>
#include <type_traits>

namespace vast {
namespace detail {

// Parses N bytes in network byte order.
template <size_t N>
struct extract;

template <>
struct extract<1> {
  template <class Iterator, class Attribute>
  static auto parse(Iterator& f, const Iterator& l, Attribute& a) -> bool {
    if (f == l)
      return false;
    a |= *f++ & 0xFF;
    return true;
  }
};

template <>
struct extract<2> {
  template <class Iterator, class Attribute>
  static auto parse(Iterator& f, const Iterator& l, Attribute& a) -> bool {
    if (!extract<1>::parse(f, l, a))
      return false;
    a <<= 8;
    return extract<1>::parse(f, l, a);
  }
};

template <>
struct extract<4> {
  template <class Iterator, class Attribute>
  static auto parse(Iterator& f, const Iterator& l, Attribute& a) -> bool {
    if (!extract<2>::parse(f, l, a))
      return false;
    a <<= 8;
    return extract<2>::parse(f, l, a);
  }
};

template <>
struct extract<8> {
  template <class Iterator, class Attribute>
  static auto parse(Iterator& f, const Iterator& l, Attribute& a) -> bool {
    if (!extract<4>::parse(f, l, a))
      return false;
    a <<= 8;
    return extract<4>::parse(f, l, a);
  }
};

} // namespace detail

namespace policy {

struct big_endian {}; // network byte order
struct little_endian {};

} // namespace policy

template <class T, class Policy = policy::big_endian, size_t Bytes = sizeof(T)>
struct byte_parser : parser_base<byte_parser<T, Policy, Bytes>> {
  using attribute = T;

  template <class Iterator>
  static auto extract(Iterator& f, const Iterator& l, T& x) -> bool {
    auto save = f;
    x = 0;
    if (!detail::extract<Bytes>::parse(save, l, x))
      return false;
    f = save;
    return true;
  }

  template <class Iterator>
  auto parse(Iterator& f, const Iterator& l, unused_type) const -> bool {
    for (auto i = 0u; i < Bytes; ++i)
      if (f != l)
        ++f;
      else
        return false;
    return true;
  }

  template <class Iterator>
  auto parse(Iterator& f, const Iterator& l, T& x) const -> bool {
    if (!extract(f, l, x))
      return false;
    if constexpr (std::is_same_v<Policy, policy::little_endian>)
      x = detail::byteswap(x);
    return true;
  }
};

template <size_t N, class T = uint8_t>
struct static_bytes_parser : parser_base<static_bytes_parser<N>> {
  static_assert(sizeof(T) == 1, "byte type T must have size 1");

  using attribute = std::array<T, N>;

  template <typename Iterator>
  auto parse(Iterator& f, const Iterator& l, std::array<T, N>& x) const
    -> bool {
    auto save = f;
    for (auto i = 0u; i < N; i++) {
      if (save == l)
        return false;
      x[i] = *save++ & 0xFF;
    }
    f = save;
    return true;
  }
};

template <class N = size_t, class T = uint8_t>
struct dynamic_bytes_parser : parser_base<dynamic_bytes_parser<N, T>> {
  static_assert(sizeof(T) == 1, "byte type T must have size 1");

  using attribute = std::vector<T>;

  dynamic_bytes_parser(const N& n) : n_{n} {
  }

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& xs) const -> bool {
    auto save = f;
    auto out = std::back_inserter(xs);
    for (auto i = N{0}; i < n_; i++) {
      if (save == l)
        return false;
      *out++ = *save++ & 0xFF;
    }
    f = save;
    return true;
  }

  template <class Iterator, size_t M>
  auto parse(Iterator& f, const Iterator& l, std::array<T, M>& xs) const
    -> bool {
    if (M < n_ || static_cast<N>(l - f) < n_)
      return false;
    for (auto i = N{0}; i < n_; i++)
      xs[i] = *f++ & 0xFF;
    return true;
  }

  const N& n_;
};

namespace parsers {

auto const byte = byte_parser<uint8_t, policy::big_endian>{};
auto const b16be = byte_parser<uint16_t, policy::big_endian>{};
auto const b32be = byte_parser<uint32_t, policy::big_endian>{};
auto const b64be = byte_parser<uint64_t, policy::big_endian>{};
auto const b16le = byte_parser<uint16_t, policy::little_endian>{};
auto const b32le = byte_parser<uint32_t, policy::little_endian>{};
auto const b64le = byte_parser<uint64_t, policy::little_endian>{};

template <size_t N, class T = uint8_t>
auto const bytes = static_bytes_parser<N, T>{};

// TODO: Make the interface coherent by replacing the above variable template
// with this function and then renaming nbytes to bytes.
//template <size_t N, class T = uint8_t>
//auto bytes() {
//  return static_bytes_parser<N, T>{};
//}
template <class T, class N>
auto nbytes(const N& n) {
  return dynamic_bytes_parser<N, T>{n};
}

} // namespace parsers
} // namespace vast

