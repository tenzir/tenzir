#ifndef VAST_CONCEPT_PARSEABLE_NUMERIC_BINARY_H
#define VAST_CONCEPT_PARSEABLE_NUMERIC_BINARY_H

#include <cstdint>
#include <type_traits>

#include "vast/concept/parseable/core/parser.h"
#include "vast/util/byte_swap.h"

namespace vast {
namespace detail {

// Parses N bytes in network byte order.
template <size_t N>
struct extract;

template <>
struct extract<1>
{
  template <typename Iterator, typename Attribute>
  static bool parse(Iterator& f, Iterator const& l, Attribute& a)
  {
    if (f == l)
      return false;
    a = *f++;
    return true;
  }
};

template <>
struct extract<2>
{
  template <typename Iterator, typename Attribute>
  static bool parse(Iterator& f, Iterator const& l, Attribute& a)
  {
    if (f == l)
      return false;
    a = *f++;
    if (f == l)
      return false;
    a <<= 8;
    a |= *f++;
    return true;
  }
};

template <>
struct extract<4>
{
  template <typename Iterator, typename Attribute>
  static bool parse(Iterator& f, Iterator const& l, Attribute& a)
  {
    if (! extract<2>::parse(f, l, a))
      return false;
    a <<= 8;
    return extract<2>::parse(f, l, a);
  }
};

template <>
struct extract<8>
{
  template <typename Iterator, typename Attribute>
  static bool parse(Iterator& f, Iterator const& l, Attribute& a)
  {
    if (! extract<4>::parse(f, l, a))
      return false;
    a <<= 8;
    return extract<4>::parse(f, l, a);
  }
};

} // namespace detail

template <typename T, endianness Endian = big_endian, size_t Bytes = sizeof(T)>
struct binary_parser : parser<binary_parser<T, Endian, Bytes>>
{
  using attribute = T;

  template <typename Iterator>
  static bool extract(Iterator& f, Iterator const& l, T& x)
  {
    auto save = f;
    if (! detail::extract<Bytes>::parse(save, l, x))
      return false;
    f = save;
    return true;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    for (auto i = 0u; i < Bytes; ++i)
      if (f != l)
        ++f;
      else
        return false;
    return true;
  }

  template <typename Iterator, endianness E = Endian>
  auto parse(Iterator& f, Iterator const& l, T& x) const
    -> std::enable_if_t<E == big_endian, bool>
  {
    return extract(f, l, x);
  }

  template <typename Iterator, endianness E = Endian>
  auto parse(Iterator& f, Iterator const& l, T& x) const
    -> std::enable_if_t<E == little_endian, bool>
  {
    if (! extract(f, l, x))
      return false;
    x = util::byte_swap<big_endian, little_endian>(x);
    return true;
  }
};

namespace parsers {

auto const b8be = binary_parser<uint8_t, big_endian>{};
auto const b16be = binary_parser<uint16_t, big_endian>{};
auto const b32be = binary_parser<uint32_t, big_endian>{};
auto const b64be = binary_parser<uint64_t, big_endian>{};
auto const b8le = binary_parser<uint8_t, little_endian>{};
auto const b16le = binary_parser<uint16_t, little_endian>{};
auto const b32le = binary_parser<uint32_t, little_endian>{};
auto const b64le = binary_parser<uint64_t, little_endian>{};

} // namespace parsers
} // namespace vast

#endif
