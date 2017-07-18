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
  template <typename Iterator, typename Attribute>
  static bool parse(Iterator& f, Iterator const& l, Attribute& a) {
    if (f == l)
      return false;
    a |= *f++ & 0xFF;
    return true;
  }
};

template <>
struct extract<2> {
  template <typename Iterator, typename Attribute>
  static bool parse(Iterator& f, Iterator const& l, Attribute& a) {
    if (!extract<1>::parse(f, l, a))
      return false;
    a <<= 8;
    return extract<1>::parse(f, l, a);
  }
};

template <>
struct extract<4> {
  template <typename Iterator, typename Attribute>
  static bool parse(Iterator& f, Iterator const& l, Attribute& a) {
    if (!extract<2>::parse(f, l, a))
      return false;
    a <<= 8;
    return extract<2>::parse(f, l, a);
  }
};

template <>
struct extract<8> {
  template <typename Iterator, typename Attribute>
  static bool parse(Iterator& f, Iterator const& l, Attribute& a) {
    if (!extract<4>::parse(f, l, a))
      return false;
    a <<= 8;
    return extract<4>::parse(f, l, a);
  }
};

} // namespace detail

namespace policy {

struct swap {};
struct no_swap {};

} // namespace policy

template <class T, class Policy = policy::no_swap, size_t Bytes = sizeof(T)>
struct byte_parser : parser<byte_parser<T, Policy, Bytes>> {
  using attribute = T;

  template <typename Iterator>
  static bool extract(Iterator& f, Iterator const& l, T& x) {
    auto save = f;
    x = 0;
    if (!detail::extract<Bytes>::parse(save, l, x))
      return false;
    f = save;
    return true;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    for (auto i = 0u; i < Bytes; ++i)
      if (f != l)
        ++f;
      else
        return false;
    return true;
  }

  template <typename Iterator, typename P = Policy>
  auto parse(Iterator& f, Iterator const& l, T& x) const
  -> std::enable_if_t<std::is_same<P, policy::no_swap>{}, bool> {
    return extract(f, l, x);
  }

  template <typename Iterator, typename P = Policy>
  auto parse(Iterator& f, Iterator const& l, T& x) const
  -> std::enable_if_t<std::is_same<P, policy::swap>{}, bool> {
    if (!extract(f, l, x))
      return false;
    x = detail::byte_swap(x);
    return true;
  }
};

template <size_t N>
struct bytes_parser : parser<bytes_parser<N>> {
  using attribute = std::array<uint8_t, N>;

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, std::array<uint8_t, N>& x) const {
    auto save = f;
    for (auto i = 0u; i < N; i++)
      if (!detail::extract<1>::parse(save, l, x[i]))
        return false;
    f = save;
    return true;
  }
};

namespace parsers {

auto const byte = byte_parser<uint8_t>{};
auto const b16be = byte_parser<uint16_t, policy::no_swap>{};
auto const b32be = byte_parser<uint32_t, policy::no_swap>{};
auto const b64be = byte_parser<uint64_t, policy::no_swap>{};
auto const b16le = byte_parser<uint16_t, policy::swap>{};
auto const b32le = byte_parser<uint32_t, policy::swap>{};
auto const b64le = byte_parser<uint64_t, policy::swap>{};

template <size_t N>
auto const bytes = bytes_parser<N>{};

} // namespace parsers
} // namespace vast

#endif
