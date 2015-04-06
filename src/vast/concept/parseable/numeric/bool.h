#ifndef VAST_CONCEPT_PARSEABLE_NUMERIC_BOOL_H
#define VAST_CONCEPT_PARSEABLE_NUMERIC_BOOL_H

#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/string/char.h"
#include "vast/concept/parseable/string/c_string.h"

namespace vast {

namespace detail {

struct single_char_bool_policy
{
  template <typename Iterator>
  static bool parse_true(Iterator& f, Iterator const& l)
  {
    return char_parser{'T'}.parse(f, l, unused);
  }

  template <typename Iterator>
  static bool parse_false(Iterator& f, Iterator const& l)
  {
    return char_parser{'F'}.parse(f, l, unused);
  }
};

struct zero_one_bool_policy
{
  template <typename Iterator>
  static bool parse_true(Iterator& f, Iterator const& l)
  {
    return char_parser{'1'}.parse(f, l, unused);
  }

  template <typename Iterator>
  static bool parse_false(Iterator& f, Iterator const& l)
  {
    return char_parser{'0'}.parse(f, l, unused);
  }
};

struct literal_bool_policy
{
  template <typename Iterator>
  static bool parse_true(Iterator& f, Iterator const& l)
  {
    return c_string_parser{"true"}.parse(f, l, unused);
  }

  template <typename Iterator>
  static bool parse_false(Iterator& f, Iterator const& l)
  {
    return c_string_parser{"false"}.parse(f, l, unused);
  }
};

} // namespace detail

template <typename Policy>
struct bool_parser : parser<bool_parser<Policy>>
{
  using attribute = bool;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    if (f == l)
      return false;
    if (Policy::parse_true(f, l))
      a = true;
    else if (Policy::parse_false(f, l))
      a = false;
    else
      return false;
    return true;
  }
};

using single_char_bool_parser = bool_parser<detail::single_char_bool_policy>;
using zero_one_bool_parser = bool_parser<detail::zero_one_bool_policy>;
using literal_bool_parser = bool_parser<detail::literal_bool_policy>;

template <>
struct parser_registry<bool>
{
  using type = single_char_bool_parser;
};

} // namespace vast

#endif
