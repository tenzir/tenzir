//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/char_class.hpp"

#include <cassert>
#include <string>

namespace vast {

/// Parser for a string surrounded by a pair of quote characters.
///
/// @note The parser has two template arguments `Esc` and `Quote`, specifying
/// the escape and quote characters used. Inside the string, the escape sequence
/// `Esc Quote` can be used to represent a literal quote character, and the
/// escape sequence `Esc Esc` can be used to represent a sequence of two
/// escape characters. All other occurences of the escape character are
/// interpreted as character literals.
///
/// For example, using backslash as escape character and double quote as quote
/// character:
///
///    "foo\n"     denotes the string ['f', 'o', 'o', '\', 'n']
///    "C:\sys32"  denotes the string ['C', ':', '\', 's', 'y', 's', '3', '2']
///    "\\"        denotes the string ['\', '\']
///    "\""        denotes the string ['"']
///    "\\""       denotes the string ['\', '\'], with a final " left unparsed
///    "\\\"       is an invalid string (unterminated)
template <char Quote, char Esc>
class quoted_string_parser
  : public parser_base<quoted_string_parser<Quote, Esc>> {
public:
  using attribute = std::string;

  static constexpr auto quote = parsers::ch<Quote>;
  static constexpr auto ignore_esc = ignore(parsers::ch<Esc>);
  static constexpr auto ignore_quote = ignore(parsers::ch<Quote>);
  static constexpr auto esc_quote = ignore_esc >> quote;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    auto esc_esc = [&]() {
      // clang-format off
      if constexpr (std::is_same_v<unused_type, Attribute>)
        return ignore_esc >> ignore_esc;
      else
        // The kleene_parser will append exactly one char per invocation to the
        // output string, so if we returned nothing from this lambda it would
        // append a default-initialized null byte.
        return (ignore_esc >> ignore_esc) ->* [&] { x.push_back(Esc); return Esc; };
      // clang-format on
    }();
    auto str_chr = esc_quote | esc_esc | (parsers::any - quote);
    auto quoted_str = ignore_quote >> *str_chr >> ignore_quote;
    return quoted_str(f, l, x);
  }
};

template <>
struct parser_registry<std::string> {
  using type = quoted_string_parser<'"', '\\'>;
};

namespace parsers {

template <char Quote, char Esc>
const auto quoted = quoted_string_parser<Quote, Esc>{};

const auto qstr = quoted<'\'', '\\'>;
const auto qqstr = quoted<'"', '\\'>;

} // namespace parsers
} // namespace vast
