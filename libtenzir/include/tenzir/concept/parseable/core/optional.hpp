//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/support/detail/attr_fold.hpp"

#include <caf/optional.hpp>

namespace tenzir {

template <class Parser>
class optional_parser : public parser_base<optional_parser<Parser>> {
public:
  using inner_attribute = detail::attr_fold_t<typename Parser::attribute>;

  using attribute
    = std::conditional_t<std::is_same_v<inner_attribute, unused_type>,
                         unused_type, std::optional<inner_attribute>>;

  constexpr explicit optional_parser(Parser p) : parser_{std::move(p)} {
    // nop
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      parser_(f, l, unused);
    } else {
      inner_attribute attr;
      if (parser_(f, l, attr))
        a = std::move(attr);
    }
    return true;
  }

private:
  Parser parser_;
};

} // namespace tenzir
