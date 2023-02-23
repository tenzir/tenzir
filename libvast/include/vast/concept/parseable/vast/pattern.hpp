//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/access.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/pattern.hpp"

namespace vast {

using slash_delimited_string = quoted_string_parser<'/', '\\'>;

template <>
struct access::parser_base<pattern>
  : vast::parser_base<access::parser_base<pattern>> {
  using attribute = pattern;

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    return slash_delimited_string{}(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, pattern& a) const {
    auto str = std::string{};
    if (!slash_delimited_string{}(f, l, str))
      return false;
    auto case_insensitive = parsers::chr{pattern::case_insensitive_flag}(f, l);
    auto result = pattern::make(std::move(str), case_insensitive);
    if (!result)
      return false;
    a = std::move(*result);
    return true;
  }
};

template <>
struct parser_registry<pattern> {
  using type = access::parser_base<pattern>;
};

namespace parsers {

static auto const pattern = make_parser<vast::pattern>();

} // namespace parsers

} // namespace vast
