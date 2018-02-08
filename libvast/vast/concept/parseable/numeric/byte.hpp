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

#ifndef VAST_CONCEPT_PARSEABLE_NUMERIC_BYTE_HPP
#define VAST_CONCEPT_PARSEABLE_NUMERIC_BYTE_HPP

#include <cstdint>
#include <type_traits>

#include "vast/detail/byte_swap.hpp"

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {
namespace detail {

// Parses N bytes in network byte order.
template <size_t N>
struct extract;

template <>
struct extract<1> {
  template <class Iterator, class Attribute>
  static bool parse(Iterator& f, const Iterator& l, Attribute& a) {
    if (f == l)
      return false;
    a |= *f++ & 0xFF;
    return true;
  }
};

template <>
struct extract<2> {
  template <class Iterator, class Attribute>
  static bool parse(Iterator& f, const Iterator& l, Attribute& a) {
    if (!extract<1>::parse(f, l, a))
      return false;
    a <<= 8;
    return extract<1>::parse(f, l, a);
  }
};

template <>
struct extract<4> {
  template <class Iterator, class Attribute>
  static bool parse(Iterator& f, const Iterator& l, Attribute& a) {
    if (!extract<2>::parse(f, l, a))
      return false;
    a <<= 8;
    return extract<2>::parse(f, l, a);
  }
};

template <>
struct extract<8> {
  template <class Iterator, class Attribute>
  static bool parse(Iterator& f, const Iterator& l, Attribute& a) {
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
struct byte_parser : parser<byte_parser<T, Policy, Bytes>> {
  using attribute = T;

  template <class Iterator>
  static bool extract(Iterator& f, const Iterator& l, T& x) {
    auto save = f;
    x = 0;
    if (!detail::extract<Bytes>::parse(save, l, x))
      return false;
    f = save;
    return true;
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    for (auto i = 0u; i < Bytes; ++i)
      if (f != l)
        ++f;
      else
        return false;
    return true;
  }

  template <class Iterator>
    bool parse(Iterator& f, const Iterator& l, T& x) const {
      if (!extract(f, l, x))
          return false;
      if constexpr (std::is_same_v<Policy, policy::little_endian>)
        x = detail::byte_swap(x);
      return true;
    }
};

template <size_t N>
struct bytes_parser : parser<bytes_parser<N>> {
  using attribute = std::array<uint8_t, N>;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, std::array<uint8_t, N>& x) const {
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

namespace parsers {

auto const byte = byte_parser<uint8_t, policy::big_endian>{};
auto const b16be = byte_parser<uint16_t, policy::big_endian>{};
auto const b32be = byte_parser<uint32_t, policy::big_endian>{};
auto const b64be = byte_parser<uint64_t, policy::big_endian>{};
auto const b16le = byte_parser<uint16_t, policy::little_endian>{};
auto const b32le = byte_parser<uint32_t, policy::little_endian>{};
auto const b64le = byte_parser<uint64_t, policy::little_endian>{};

template <size_t N>
auto const bytes = bytes_parser<N>{};

} // namespace parsers
} // namespace vast

#endif
