//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"

#include <string>
#include <type_traits>
#include <utility>

namespace tenzir {

/// Captures the raw input consumed by a parser as a string.
template <class Parser>
class raw_parser : public parser_base<raw_parser<Parser>> {
public:
  using attribute = std::string;

  constexpr explicit raw_parser(Parser p) : parser_{std::move(p)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    auto first = f;
    if (not parser_(f, l, unused)) {
      f = first;
      return false;
    }
    if constexpr (not std::is_same_v<Attribute, unused_type>) {
      a = attribute{first, f};
    }
    return true;
  }

private:
  Parser parser_;
};

template <parser Parser>
constexpr auto raw(Parser&& p) {
  return raw_parser<std::decay_t<Parser>>{std::forward<Parser>(p)};
}

} // namespace tenzir
